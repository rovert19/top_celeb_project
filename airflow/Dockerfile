FROM quay.io/astronomer/astro-runtime:12.6.0

USER root

# RUN apt-get update && apt-get install -y chromium chromedriver

RUN apt-get update && apt-get install -y wget unzip && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean
