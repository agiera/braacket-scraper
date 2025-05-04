from bs4 import BeautifulSoup
import requests
import polars

url = 'https://braacket.com/league/nemelee/tournament?rows=20'

tournaments = []
matches = []

response = requests.get(url)
tournament_soup = BeautifulSoup(response.content, 'html.parser')
tournament_tags = tournament_soup.find_all(class_='panel')

for tournament_tag in tournament_tags:
    data = {
        'id': tournament_tag.get('id', None),
        'url': (tournament_tag.find('a', class_='panel-heading')
                ['href'] if tournament_tag.find(class_='panel-heading.game_bg') else None),
        'name': (tournament_tag.find('a', class_='panel-heading')
                 ['href'] if tournament_tag.find(class_='panel-heading.game_bg') else None),
        'date': (tournament_tag.find(text="Date")
                 .next_sibling.text.strip() if tournament_tag.find(text="Date") and tournament_tag.find(text="Date").next_sibling else None),
        'country': (tournament_tag.find(class_='country_flag').get('src', None)
                    if tournament_tag.find(class_='country_flag') else None),
        'region': (tournament_tag.find(class_='country_region_flag').get('src', None)
                   if tournament_tag.find(class_='country_region_flag') else None),
        'number_of_players': (int(tournament_tag.find(data_original_title_='Imported players').text.strip())
                              if tournament_tag.find(data_original_title_='Imported players') else None)
    }
    tournaments.append(data)

# Convert the list of dictionaries to a Polars DataFrame
tournaments_df = polars.DataFrame(tournaments)
# Save the DataFrame to a CSV file
print(tournaments_df)
