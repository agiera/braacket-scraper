import argparse
import os
from bs4 import BeautifulSoup
import requests
import polars as pl
import re


braacket_url = 'https://braacket.com'
match_schema_overrides = {
    'encounter_id': pl.Int64,
    'winner_score': pl.Int64,
    'loser_score': pl.Int64,
    'tournament_date': pl.Date,
    'tournament_number_of_players': pl.Int64,
}


def tournaments_url_from_league(league_name: str) -> str:
    return f'{braacket_url}/league/{league_name}/tournament?rows=200'


def scrape_tournament_tags(tournaments_url):
    response = requests.get(tournaments_url)
    tournament_soup = BeautifulSoup(response.content, 'html.parser')
    num_pages = (
        tournament_soup
        .find(class_='search-pagination')
        .find_all(class_='input-group-addon')[-1]
        .text.split(' ')[-1].strip()
    )

    for i in range(1, int(num_pages) + 1):
        tournaments_page_url = f'{tournaments_url}&page={i}'
        response = requests.get(tournaments_page_url)
        tournament_soup = BeautifulSoup(response.content, 'html.parser')
        tournament_tags = list(
            map(
                lambda x: x.find_parent(class_='panel'),
                tournament_soup.find_all(string='Detail')
            )
        )
        for tag in tournament_tags:
            yield tag


def extract_tournament_data(tournament_tags):
    for tournament_tag in tournament_tags:
        yield {
            'url': tournament_tag.find(class_='panel-heading').find('a')['href'],
            'name': tournament_tag.find(class_='panel-heading').find('a').text.strip(),
            'date': tournament_tag.find(string='Date').parent
                     .find_next_sibling().text.strip() if tournament_tag.find(string='Date').parent.find_next_sibling() else None,
            'country': (tournament_tag.find(class_='country_flag').find('img').get('src', None)
                        if tournament_tag.find(class_='country_flag') else None),
            'region': (tournament_tag.find(class_='country_region_flag').get('src', None)
                       if tournament_tag.find(class_='country_region_flag') else None),
            'number_of_players': (int(tournament_tag.find(data_original_title_='Imported players').text.strip())
                                  if tournament_tag.find(data_original_title_='Imported players') else None)
        }


def scrape_stage_urls(match_url):
    response = requests.get(match_url)
    match_soup = BeautifulSoup(response.content, 'html.parser')
    stage_urls = list(set(
        map(
            lambda x: x['href'],
            match_soup.find_all(attrs={"href": re.compile(r"^/tournament/.*/stage/.*$")}),
        )
    ))
    return stage_urls


def scrape_player_url(league_name: str, player_url: str) -> str:
    response = requests.get(f'{braacket_url}/{player_url}')
    player_soup = BeautifulSoup(response.content, 'html.parser')

    search_term = rf"^/league/{league_name}/player/.*$"
    league_links = player_soup.find_all(attrs={"href": re.compile(search_term)})

    if not league_links:
        return None

    player_url = league_links[0]['href']
    return player_url


def scrape_stage_matches(league: str, tournament: dict, stage_url: str) -> list:
    matches = []
    response = requests.get(f'{braacket_url}/{stage_url}')
    stage_soup = BeautifulSoup(response.content, 'html.parser')

    player_url_dict = {}
    stage_encounters = stage_soup.find_all(class_='tournament_encounter-row')
    for encounter in stage_encounters:
        winner_encounter = encounter.select('.tournament_encounter_opponent.winner')[0]
        loser_encounter = encounter.select('.tournament_encounter_opponent.loser')[0]

        winner_tournament_url = winner_encounter.select('a')[0]['href']
        loser_tournament_url = loser_encounter.select('a')[0]['href']

        if winner_tournament_url not in player_url_dict:
            player_url_dict[winner_tournament_url] = \
                scrape_player_url(league, winner_tournament_url)
        if loser_tournament_url not in player_url_dict:
            player_url_dict[loser_tournament_url] = \
                scrape_player_url(league, loser_tournament_url)

        data = {
            'encounter_id': encounter.find(class_='tournament_encounter-id').text.strip(),
            'winner_url': player_url_dict[winner_tournament_url],
            'winner': winner_encounter.text.strip(),
            'loser_url': player_url_dict[loser_tournament_url],
            'loser': loser_encounter.text.strip(),
            'winner_score': encounter.select('.tournament_encounter-score.winner')[0].text.strip(),
            'loser_score': encounter.select('.tournament_encounter-score.loser')[0].text.strip(),
        }
        data.update({
            f'tournament_{k}': v
            for k, v in tournament.items()
        })
        matches.append(data)
    return matches


def main(args: argparse.ArgumentParser):
    league_name = args.league_name
    tournaments_url = tournaments_url_from_league(league_name)

    tournament_tags = scrape_tournament_tags(tournaments_url)
    tournaments = extract_tournament_data(tournament_tags)

    if not os.path.exists('data/matches.csv'):
        args.update = "full"
    elif args.update == "new":
        matches = pl.read_csv('data/matches.csv', schema_overrides=match_schema_overrides)

    new_matches_list = []
    for tournament in tournaments:
        # Skip tournaments already in the matches file
        condition_expr = (pl.col('tournament_url') == tournament['url']).any()
        if matches.select(condition_expr).item():
            # quit if we're just looking for new tournaments
            if args.update == "new":
                break
            continue

        tournament_matches = []
        print(f"Scraping matches for tournament {tournament['name']}, {tournament['url']}")
        try:
            match_url = f'https://braacket.com/{tournament["url"]}/match'
            stage_urls = scrape_stage_urls(match_url)
            for stage_url in stage_urls:
                tournament_matches += scrape_stage_matches(args.league_name, tournament, stage_url)
        except Exception as e:
            error_details = f'{tournament['name']}, {tournament['url']}: {e}'
            print(f"Error processing tournament {error_details}")
            continue
        new_matches_list += tournament_matches
    
    if not new_matches_list:
        print('no new tournaments found')
        return

    matches = (
        pl.DataFrame(
            new_matches_list,
            schema_overrides={
                **match_schema_overrides,
                'tournament_date': pl.String,
            },
        )
        .with_columns(pl.col('tournament_date').str.strptime(pl.Date, format='%d %B %Y'))
        .vstack(matches)
        .unique()
    )
    matches.write_csv('data/matches.csv')


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Braacket Scraper")
    parser.add_argument("--league_name", type=str, default='nemelee', help="Name of the league to scrape")
    parser.add_argument("--update", choices=["full", "new"], default='new', help="Update mode: 'new' updates all tournaments and matches, 'new' updates only new tournaments and matches")
    args = parser.parse_args()

    main(args)
