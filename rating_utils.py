import trueskill
import polars as pl

from typing import Tuple


def calculate_ratings(matches: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    players_dict = {}

    def rate_players(row: dict) -> dict:
        # Create TrueSkill rating objects for the players
        if players_dict.get(row['winner_url']) is None:
            players_dict[row['winner_url']] = trueskill.Rating()
        if players_dict.get(row['loser_url']) is None:
            players_dict[row['loser_url']] = trueskill.Rating()

        result = {
            'winner_rating': players_dict[row['winner_url']],
            'loser_rating': players_dict[row['loser_url']],
        }

        # Update the ratings based on the match outcome
        new_winner_rating, new_loser_rating = trueskill.rate_1vs1(
            players_dict[row['winner_url']], players_dict[row['loser_url']])
        players_dict[row['winner_url']] = new_winner_rating
        players_dict[row['loser_url']] = new_loser_rating

        result.update({
            'new_winner_rating': new_winner_rating,
            'new_loser_rating': new_loser_rating,
        })
        return result

    matches_with_rating = (
        matches
        .with_columns(
            pl.struct(['winner_url', 'loser_url'])
                .map_elements(rate_players, return_dtype=pl.Struct)
                .alias('new_ratings')
        ).unnest('new_ratings')
    )

    player_ratings = pl.DataFrame({
        'url': [player_url for player_url in players_dict.keys()],
        'rating': [rating.mu for rating in players_dict.values()],
        'rating_95ci_lower': [rating.mu - 2*rating.sigma for rating in players_dict.values()],
        'rating_95ci_upper': [rating.mu + 2*rating.sigma for rating in players_dict.values()],
        'sigma': [rating.sigma for rating in players_dict.values()],
        'pi': [rating.pi for rating in players_dict.values()],
        'tau': [rating.tau for rating in players_dict.values()],
        'exposure': [rating.exposure for rating in players_dict.values()],
    }).join(players, on='url').sort('rating', descending=True)
