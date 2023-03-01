# Reporting-automation-ver_1

We solve the problem of automating the basic reporting of our application: every morning in Telegram analysis will be sent.\
A script was written to build a report on the news feed.\
The report consists of two parts:
  - text with information about the values of key metrics (DAU, Views, Likes, CTR) for the previous day;
  - graph with metric values for the previous 7 days.
>
Automation of sending the report was made using **Airflow**.

Fields of the feed_actions table:\
	- user_id\
	- post_id\
	- action\
	- time\
	- gender\
	- age\
	- country\
	- city\
	- os\
	- source\
	- exp_group
