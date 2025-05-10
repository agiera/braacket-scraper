import trueskill
import polars as pl

from typing import Optional


def player_map(matches: pl.DataFrame) -> pl.DataFrame:
    """returns a DataFrame with distinct player urls and a tag for each"""
    return (
        matches
        .select(
            pl.col('winner').alias('tag'),
            pl.col('winner_url').alias('url'),
            'tournament_date',
        )
        .vstack(
            matches
            .select(
                pl.col('loser').alias('tag'),
                pl.col('loser_url').alias('url'),
                'tournament_date',
            )
        )
        .unique()
        .sort('tournament_date', descending=False)
        .group_by('url')
        .agg(pl.col('tag').mode().first())
        .select('tag', 'url')
    )

def stats_from_rating(rating: trueskill.Rating) -> dict:
    """returns a dictionary with the rating statistics"""
    return {
        'rating': rating.mu,
        'rating_95ci_lower': rating.mu - 2*rating.sigma,
        'rating_95ci_upper': rating.mu + 2*rating.sigma,
        'sigma': rating.sigma,
        'pi': rating.pi,
        'tau': rating.tau,
        'exposure': rating.exposure,
    }

def get_final_ratings(matches: pl.DataFrame) -> pl.DataFrame:
    """
    Get the final ratings for players based on match results.
    Args:
        matches (pl.DataFrame): DataFrame containing match results.
    Returns:
        pl.DataFrame: DataFrame containing player ratings and statistics.
    """
    players = player_map(matches)
    return (
        matches
        .select([
            pl.col(col).alias(col.replace('winner_', ''))
            for col in matches.columns
            if 'winner_' in col
        ] + [
            'tournament_date', 'encounter_id',
        ])
        .vstack(
            matches
            .select([
                pl.col(col).alias(col.replace('loser_', ''))
                for col in matches.columns
                if 'loser_' in col
            ] + [
                'tournament_date', 'encounter_id',
            ])
        )
        .sort('tournament_date', 'encounter_id')
        .group_by('url').last()
        .join(players, on='url', how='left')
        .select(pl.exclude('score'))
        .sort('rating', descending=True)
    )

def calculate_ratings(
        matches: pl.DataFrame,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sigma_fix: Optional[float] = None,
    ) -> pl.DataFrame:
    """
    Calculate TrueSkill ratings for players based on match results.
    Args:
        matches (pl.DataFrame): DataFrame containing match results.
        start_date (str, optional): Start date for filtering matches.
        end_date (str, optional): End date for filtering matches.
        sigma_fix (float, optional): Use all time rating for players with a sigma higher than this.
    Returns:
        pl.DataFrame: DataFrame containing player ratings and statistics.
    """

    matches = matches.filter(
        pl.col('winner_url').is_not_null(),
        pl.col('loser_url').is_not_null(),
    )

    if sigma_fix is not None and (start_date is None or end_date is None):
        raise ValueError("""sigma_fix only makes sense in the context of rating
            low activity players within a season.
            sigma_fix requires start_date or end_date to be set.""")

    # Using these variables outside `rate_players` scope
    # This way we can access them after or use them as input
    fixed_players = pl.DataFrame({'url': []})
    player_rating_dict = {}
    def rate_players(row: dict) -> dict:
        # Create TrueSkill rating objects for the players
        if player_rating_dict.get(row['winner_url']) is None:
            player_rating_dict[row['winner_url']] = trueskill.Rating()
        if player_rating_dict.get(row['loser_url']) is None:
            player_rating_dict[row['loser_url']] = trueskill.Rating()

        result = {
            'winner_starting_rating': player_rating_dict[row['winner_url']],
            'loser_starting_rating': player_rating_dict[row['loser_url']],
        }

        # Update the ratings based on the match outcome
        new_winner_rating, new_loser_rating = trueskill.rate_1vs1(
            player_rating_dict[row['winner_url']], player_rating_dict[row['loser_url']])
        
        # revert to the original rating if the player is in the fixed players list
        if fixed_players.filter(pl.col('url') == row['winner_url']).height > 0:
            new_winner_rating = player_rating_dict[row['winner_url']]
        if fixed_players.filter(pl.col('url') == row['loser_url']).height > 0:
            new_loser_rating = player_rating_dict[row['loser_url']]

        # Update the player ratings in the dictionary
        player_rating_dict[row['winner_url']] = new_winner_rating
        player_rating_dict[row['loser_url']] = new_loser_rating

        result.update({
            f'winner_{k}': v
            for k, v in stats_from_rating(new_winner_rating).items()
        } | {
            f'loser_{k}': v
            for k, v in stats_from_rating(new_loser_rating).items()
        })
        return result
    
    def matches_to_rating_df(matches: pl.DataFrame) -> pl.DataFrame:
        return (
            matches
            .with_columns(
                pl.struct(['winner_url', 'loser_url'])
                    .map_elements(rate_players, return_dtype=pl.Struct)
                    .alias('new_ratings')
            ).unnest('new_ratings')
        )

    # Filter matches based on start_date and end_date
    if start_date is None and end_date is None:
        matches_subset = matches
    else:
        matches_subset = (
            matches
            .filter(
                (start_date is None) or (pl.col('tournament_date') >= start_date),
                (end_date is None) or (pl.col('tournament_date') <= end_date),
            )
        )

    # calculate initial ratings for date range
    fixed_players = pl.DataFrame({'url': []})
    player_rating_dict = {}
    matches_subset_with_rating = matches_to_rating_df(matches_subset)

    # sigma fix doesn't apply
    if sigma_fix is None or (start_date is None and end_date is None):
        return matches_subset_with_rating

    # sigma fix must be applied
    # calculate all time ratings
    fixed_players = pl.DataFrame({'url': []})
    player_rating_dict = {}
    matches_to_rating_df(matches)

    # Recalculate ratings based on the sigma fix
    fixed_players = (
        get_final_ratings(matches_subset_with_rating)
        .filter(pl.col('sigma') > sigma_fix)
        .select('url')
    )
    player_rating_dict = {
        k: v
        for k, v in player_rating_dict.items()
        if fixed_players.filter(pl.col('url') == k).height > 0
    }
    return matches_to_rating_df(matches_subset)


if __name__ == '__main__':
    # Example usage
    matches = pl.read_csv('data/matches.csv')
    ratings = calculate_ratings(
        matches,
        start_date='2023-01-01', end_date='2023-12-31',
        sigma_fix=2.0,
    )
    print(get_final_ratings(ratings))
