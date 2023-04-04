# Xeptagon Onchain Analytics
  Backend architecture of Xeptagon opensource Onchain analytics using data obtained from blockdata and blockchair. The OnChain Analytics platform provides users with two categories of metrics, basic and computed. Basic metrics are those whose data is readily available without any need for additional computation, while computed metrics are those whose values are derived from the computation of basic metrics. For example, Market Value to Realized Value (MRVR) ratio to understand whether a cryptocurrency is undervalued or overvalued, Spent Output Profit Ratio (SOPR) to identify whether a crypto coin was sold at a profit or loss, and Relative Unrealized Profit/Loss to gauge investor's sentiment, are few of the wide range of metrics offered. These metrics have been consolidated based on their relevance and usefulness.  
  
 Read More From [Open-source On-Chain Analytics Platform by Xeptagon](https://www.xeptagon.com/blog-xeptagon-open-source-onchain-analytics-framework.html)

# System Architecture
Architecture for Metrics Calculation present in Following diagram.
![System Architecture for Metrics Calculation](https://github.com/xeptagondev/xep-onchain-internal/blob/main/Readme.png)

# Running The Script
1. Install Dependencies:
    ```
    pip3 install -r requirements.txt 
    ```
 2. Put the Enviroment Variables as per your interest:

    ```
      AWS_ACCESS_KEY_ID: <Put Your AWS Access Key>
      AWS_SECRET_ACCESS_KEY: <Put Your AWS Secret Access Key>
      PATH_VAR: <Put Your Path to Local Storage or Path AWS Bucket(or Key If any)>
      POST_ENGINE: <Put Your DB Engine>
      POST_DATABASE: <Put Your DB Name>
      POST_HOST: <Put Your DB Host>
      POST_USER: <Put Your DB User>
      POST_PASSWORD: <Put Your DB Password>
      POST_PORT: <Put Your DB Port>
    ```
     AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY not neccessary if you're running on local storage & PATH_VAR should be path to your desired location to store downloded contents.<br>
     If you're running on Docker update the envioment variables in docker-compose.yml also Use 
     ```
     POST_HOST:'host.docker.internal' 
     ```
   3. Run the Programme:<br><br>
    After Complete Above Steps Execute:<br>
    ```
    python main.py
    ```
    <br><br>
    If you're excuting on Docker:<br>
    ```
    docker-compose up --build   
    ```
    
