FROM python:3.8


RUN apt-get update
RUN apt-get install -y gconf-service wget libasound2 libgbm1 libappindicator3-1 libatk1.0-0 libcairo2 libcups2 libfontconfig1 libgdk-pixbuf2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 libxss1 fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils && \
    apt-get install -y awscli
# # Install Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install


# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

#Fixing Webdriver issue
ENV DISPLAY=:99

# Copy direcotry
WORKDIR /on-chain
COPY ./ .

RUN pip install -r requirements.txt
CMD ["python","./main.py"]
