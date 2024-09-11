# Data Engineering Take-Home Challenge

Your challenge is help our Business Development team increase their conversion rate by implementing an alert system based on buying intent

# Assignments

### 1. Derive a customer segmentation based on high, medium, or low buying-intent (~1h)

- We've exported some data from our (fictitious) CRM into a local SQL database (`data/db_src.duckdb`) for you to work with. Before you start, please copy it to `data/db.duckdb` and leave the original file unchanged.
- The data contains information about entities (companies, contacts, etc.), as well as activities related to those entities (emails, website visits, news publications, etc.)
- Please use the following rules to segment companies based on buying intent:
  - HIGH: At least one website visit and at least one news publication within the last 7 days
  - MEDIUM: At least one news publication within the last 30 days
  - LOW: If none of the above applies
- We've created a notebook `notebooks/segmentation.ipynb` with further instructions where you can complete the assignment
- For the purpose of this assignment, please assume the **current date to be April 30th, 2024**

### 2. Validate email addresses for all companies with HIGH buying intent (~1h)

- Please use an email verification API to validate email addresses for all companies with HIGH buying intent and write the validated email addresses back into the CRM database
- We've created a notebook `notebooks/email_validation.ipynb` with further instructions where you can complete the assignment

### 3. Write a python automation to send a alert messages to our Business Development team for all contacts with HIGH buying intent AND a valid email address (~1h)

- Fee free to use https://webhook.site/ in lieu of an actual messaging API.
- Please abide by the following structure for the messaging payload for your POST request:
  ```
  {
      "from": "<from-email-address>",
      "to": "biz-dev@company.com",
      "message": "<message-goes-here>",
      "subject": "<subject-goes-here>"
  }
  ```
- We've already created an empty `alerts` module for you to implement your automation in python.
- Your program should be be executable from the command line and should be ready for being deployed into a production environment.
- We give bonus points to candidates who implement the alerting automation with an actual messaging API like GMail or Slack.
- NOTE: Please exclude any contacts who've already received an outreach message from our Business Development team within the last 14 days (please assume the current date to be April 30th, 2024)

### 4. Draw a simple architecture diagram showing how you would deploy your alerting automation into a production environment (~0.5h)

Once you've completed all assignments, please commit your code (including the modified database file) to a separate branch of this repository. We're really looking forward to your solution!

# Objectives

We give roughly equal weight to each of the following:

- Simplicity
- Reliability
- Readability
- Modularity
- Creative thinking

Please don't stress if your solution isn't perfect, or is incomplete. With the given time constraints, you won't be able to tick all the boxes above. This challenge is intended as an exercise in tradeoffs. We want to understand how you approach data problems under real-world time constraints.
