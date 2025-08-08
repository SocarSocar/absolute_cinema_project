import requests
from bs4 import BeautifulSoup

def get_reviews(imdb_id):
    url = f"https://www.imdb.com/title/{imdb_id}/reviews"
    headers = {"Accept-Language": "fr-FR,fr;q=0.9", "User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    reviews = []
    articles = soup.find_all('article', class_='sc-12fe603f-1 dHhKiF user-review-item')

    for article in articles:
        if article.find('button', class_='review-spoiler-button'):
            continue

        # Note
        note_tag = article.find('span', attrs={'aria-label': lambda x: x and 'Note de' in x})
        rating = int(note_tag['aria-label'].split(':')[1].strip()) if note_tag else None

        # Titre + commentaire
        title_tag = article.find('div', attrs={'data-testid': 'review-summary'})
        title = title_tag.get_text(strip=True) if title_tag else ""
        comment_tag = article.select_one('div.ipc-html-content.ipc-html-content--base')
        comment = comment_tag.get_text(strip=True) if comment_tag else ""
        full_comment = f'"{title}": {comment}'

        reviews.append({
            'rating': rating,
            'comment': full_comment
        })

    return reviews

# Exemple d'appel
if __name__ == "__main__":
    imdb_id = 'tt9243946'  # El Camino
    result = get_reviews(imdb_id)
    for r in result:
        print(f"Note: {r['rating']}")
        print(f"Commentaire: {r['comment']}")
        print('---')
