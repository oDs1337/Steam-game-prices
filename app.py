from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

COUNTRIES = ['us', 'gb', 'de', 'fr', 'pl', 'ru', 'br', 'au', 'jp', 'kr', 'ca', 'se', 'no', 'cz', 'hu', 'cn', 'it', 'es', 'nl', 'mx']

@app.route('/api/get_prices', methods=['POST'])
def get_prices():
    data = request.json
    app_id = data.get('app_id')
    if not app_id:
        return jsonify({'error': 'No app_id provided'}), 400

    prices = []
    for cc in COUNTRIES:
        try:
            url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&cc={cc}"
            response = requests.get(url, timeout=3)
            res_json = response.json()
            game_data = res_json.get(str(app_id), {})
            if game_data.get('success') and 'data' in game_data:
                price_info = game_data['data'].get('price_overview', {})
                final_price = price_info.get('final')
                if final_price:
                    prices.append({
                        'country': cc.upper(),
                        'currency': price_info.get('currency'),
                        'price': final_price / 100
                    })
        except Exception as e:
            continue

    sorted_prices = sorted(prices, key=lambda x: x['price'])
    print(sorted_prices)
    return jsonify(sorted_prices)