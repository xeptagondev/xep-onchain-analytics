FROM public.ecr.aws/lambda/python:3.11

# Copy requirements.txt
COPY requirements.txt ./

# Copy function code
COPY lambda_function.py ./
COPY config.json ./
# Install the specified packages
RUN pip install -r requirements.txt

# Install Chrome and Chrome WebDriver
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update -y && apt-get install -y google-chrome-stable

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.handler" ]