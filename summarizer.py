import logging
import os

from src.main.loader import ResultLoader
from flask import Flask

logging.basicConfig(level="DEBUG")
#logging.FileHandler('/var/tmp/myapp.log')


logging.info("starting summarizer")

app = Flask(__name__)


ResultLoader.register(app)


if __name__ == "__main__":
    print(app.url_map)
    app.run(host='0.0.0.0', port=os.getenv('FLASK_PORT', 5005))

