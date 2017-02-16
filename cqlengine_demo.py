#!/usr/bin/env python

"""
Example of using cassandra.cqlengine data model.

Prepare the database in vagrant first:

cqlsh> CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
cqlsh> use demo;
cqlsh:demo> CREATE TABLE users ( lastname text PRIMARY KEY, age int, city text, email text, firstname text);
"""

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


# Define a model
class Users(Model):
  firstname = columns.Text()
  age = columns.Integer()
  city = columns.Text()
  email = columns.Text()
  lastname = columns.Text(primary_key=True)
  def __repr__(self):
    return '%s %d' % (self.firstname, self.age)


from cassandra.cqlengine import connection

# Connect to the demo keyspace on our cluster running at vagrant
connection.setup(['127.0.0.1'], "demo", port=19042, cql_version='3.1.7')

from cassandra.cqlengine.management import sync_table

# Sync your model with your cql table
sync_table(Users)

# Create a row of user info for Bob
Users.create(firstname='Bob', age=35, city='Austin', email='bob@example.com', lastname='Jones')

# Read Bobs information back and print
q = Users.get(lastname='Jones')

print q.age

# Update Bobs age and then print to show the change
q.update(age=36)

print q.age

# Delete Bob, then try to print all rows to show the user is gone
q.delete()

q = Users.objects()

for i in q: print i.lastname, i.city, i.age
