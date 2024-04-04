ileads_lead_generation
==============================

lead generation module

Project Organization
------------
    ├── README.md          <- The top-level README for developers using this project.
    │
    │
    ├── docs               <- Flowcharts for all business and Sphinx documentation for details
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is business, database names and description
    |
    ├──                       `-` delimited description, e.g.
    |
    │                         `1.0-dcpd-installbase-data-exploration`.
    |___ config            <- Configuration file.  filename: "" indicates that the latest file inside the directory will be read.
    │
    ├── references         <- Referenece files. Reference file changes would be though JIRA ticket only.
    │
    ├── results            <- Generated analysis as csv, HTML, PDF, LaTeX, etc.
    │    
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment.
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    │    
    ├── Lead Generation
    │   ├── __init__.py    <- generates lead for given business
    |
    │   ├── __main__.py    <- generates lead for given business 
    │   |
    │   └── utils         <- utility functions specific to <algorithm>
    │     └── io_adopter  <- handles all read and write operations  
    │     └── <business>  <- handles business specific logic
    │   
    ├── tests
    │
    |___TimerTrigger      <-Timer Trigger azure function for implementing modules InstallBase , Contract , Services, Contacts and 
    |                       Lead Generation in order. Scheduled trigger every day.
    |
    ├── README.md
    │
    ├── version.py
    │
    └── .gitignore
    │
    └── .env                <- File containing key value pairs of all the environment variables required by your application.