rrd-tool
========
This simple command-line tool stores floating-point values in two round-robin database, one for each minute over an hour, and one for each hour over 24 hours.

Design Considerations
=====================
The timestamps are passed in in seconds-since-epoch format. Since our database is designed to be used to store values every minute, it is assumed that timestamp values not corresponding exactly to M:00 seconds should be truncated to the nearest minute, and the timestamp value for that stored in the database. This means that a timestamp may be saved in the database with the command ``rrd save 1483967112, 100.0`` but the value actually stored in the database will be at timestamp 1483967100 (the 12 seconds being truncated off). To ensure consistent behaviour, this also means that a timestamp corresponding to e.g. 14:12:59 will be truncated to be equivalent to 14:12:00.

It is also assumed that the values stored in the minutes database represent the values for the last 60 minutes (breaking over the hour mark if necessary), but that the hour database is populated from the minimum values each hour between the 00 and 59 minute mark. This means that the most recent hour will hold the values from minute 00 until the present minute, but will be updated with any saves to the database until after minute 59.

It is also assumed that reading values from the database should be optimised over saving values. This is because a standard use-case for this type of software is to store values from some kind of logging service, and there will only be one data saved per minute, at most. However, there may be many consumers of the data (such as multiple users viewing a website that generates a graph based on the data), so queries should be fast.

Usage
=====
**Command-line parameters**

    ``rrd save <epoch_timestamp> <float_to_save>``

Saves a float in the RRD at the specified timestamp. Epoch timestamp must be greater than previously saved timestamp, and intermediate values are updated to be NULL.

    ``rrd query [minutes|hours]``

Queries the specified database, returning all saved values (NULL for empty values) up to the last-saved value. Includes a summary of information at the end.

**Example output:**

::

    1483967100, 98.9
    1483967160, 101.2
    1483967220, NULL
    ...
    1483967660, 67.4
    minutes: min: 67.4, avg: 120.3, max: 152.2



