FROM python:3.11-slim

WORKDIR /app
# torch en version CPU (beaucoup plus leger, pas besoin du GPU pour predire une action a la fois)
RUN pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir numpy==1.26.4 kafka-python==2.0.6 psycopg2-binary==2.9.9

COPY temps_reel/prediction.py modele/lstm.py modele/lstm_nba.pt ./

CMD ["python", "-u", "prediction.py"]
