from flask import Flask, render_template

app = Flask(__name__)

@app.route('/ru')
def main_ru():
    return render_template('main_ru.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
    