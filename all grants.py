from flask import Flask, render_template
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__, template_folder='templates')

def generate_static_html():
    csv_path = os.path.join('data', 'doge-grant-stub.csv')
    try:
        df_all = pd.read_csv(csv_path)
    except Exception as e:
        print(f"<h2>Error loading CSV file: {e}</h2>")
        return

    df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce').dt.date
    second_row_date = df_all['date'].iloc[1] if len(df_all) > 1 else "N/A"
    last_any_grant_date = df_all['date'].max() if not df_all.empty else "N/A"

    # No filtering—show all entries
    df = df_all.copy()

    # Counts for display
    total_entries = len(df_all)
    filtered_entries = len(df)  # This is now equal to total_entries

    def clean_description(text, link, word_limit=50):
        if not isinstance(text, str):
            return ""
        words = text.split()
        truncated = ' '.join(words[:word_limit]) + ('...' if len(words) > word_limit else '')
        if truncated:
            truncated = truncated.lower()
            truncated = truncated[0].upper() + truncated[1:]
        if isinstance(link, str) and link.strip():
            truncated += f' <a href="{link}" target="_blank">[LINK]</a>'
        return truncated

    df.loc[:, 'description_doge'] = df.apply(
        lambda row: clean_description(row['description_doge'], row.get('link')), axis=1
    )

    def format_dollar(x):
        try:
            return f"${int(round(float(x))):,}"
        except:
            return x

    df.loc[:, 'value'] = df['value'].apply(format_dollar)
    df.loc[:, 'savings'] = df['savings'].apply(format_dollar)
    df.loc[:, 'date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

    last_scraped = datetime.now().strftime('%Y-%m-%d')

    with app.app_context():
        html = render_template(
            'index.html',
            grants=df.to_dict(orient='records'),
            last_scraped=last_scraped,
            second_row_date=second_row_date,
            last_hawaii_univ_date=last_any_grant_date,  # Renamed for template compatibility
            total_entries=total_entries,
            filtered_entries=filtered_entries
        )

    os.makedirs('docs', exist_ok=True)
    with open('docs/index.html', 'w', encoding='utf-8') as f:
        f.write(html)

if __name__ == '__main__':
    generate_static_html()
