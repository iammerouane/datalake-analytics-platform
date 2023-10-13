FROM python:3.9-slim

# Définissez le répertoire de travail dans le conteneur
WORKDIR /app

# Copiez les fichiers nécessaires dans le conteneur
COPY app.py /app/
COPY requirements.txt /app/

# Installez les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Exposez le port sur lequel l'application s'exécutera
EXPOSE 5000

# Commande à exécuter lorsque le conteneur démarre
CMD ["python", "app.py"]
