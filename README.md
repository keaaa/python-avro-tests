# Avro examples

## NaN issue

Prerequisite
- create event hub to send the message to. 
- create adx listening to event hub receiving avro file with tables

run app.py for sending a message to event hub. 


query that behaves weird:

```
features
| mv-expand data;
```

The mv-expand does not manage to expand the dynamic array, as it contains a NaN value. 

If running the same ingest but with no NaN value, the mv-expand works as expected. 
