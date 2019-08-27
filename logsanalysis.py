#!/usr/bin/env python
# logs Analysis - Internal Reporting Program
# Created By: Nidhi Malhan
# Project 1: Udacity Full-Stack Nanodegree

# Importing library

import psycopg2

DBNAME = "news"

# Query 1 finds the top three articles based on the highest number of views

query1 = """select a.title, count(l.path) as views from articles a left join log l
on l.path like concat('%', a.slug, '%')
group by a.title
order by views
desc limit 3;"""

# Query 2 finds the top authors based on the most popular articles

query2 = """select auth.name, count(l.path) as most_views
from authors auth, articles a, log l
where a.author = auth.id and l.path like concat('%',a.slug, '%')
and status like '%200%'
group by auth.name
order by most_views desc;"""

# Query 3 below utilizes three views created in the db to find the number of
# days where more that 1% of requests lead to errors
# Views created - Total_Errors; Total_Requests; Error_Percentage
# Please refer to the README file for documention of the VIEWS created

query3 = """select * from error_percentage where all_errors >=1;"""

# This function is to establish a connection


def connect(query):
    # Make a connection to db
    database = psycopg2.connect(database=DBNAME)
    conn = database.cursor()
    conn.execute(query)
    results = conn.fetchall()

    # close the connection
    database.close()
    return results

# This function finds the most popular articles


def popular_articles(query):
    output = connect(query)
    print("\n Top three articles of all time are: \n ")

    for r in output:
        print('\t' + str(r[0]) + ' ' + str(r[1]) + ' ' + 'views')


# This function finds the most popular authors


def popular_authors(query):
    output = connect(query)
    print("\n Most popular articles authors of all time are: \n ")

    for r in output:
        print('\t' + str(r[0]) + ' ' + str(r[1]) + ' ' + 'views')

# This function finds the number of days when more than 1%
# of requests lead to errors


def error_occured(query):
    output = connect(query)
    print("\n More than 1% of requests lead to errors was on:\n ")

    for r in output:
        print('\t' + str(r[0]) + ' ' + '-' + ' ' +
              str("%.2f" % round(r[1], 2)) + '%' + ' ' + 'errors')

if __name__ == '__main__':

    # Printing the output

    popular_articles(query1)
    popular_authors(query2)
    error_occured(query3)
