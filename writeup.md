# Design decisions

# Missing/incomplete elements of code
All of the code is complete; 100% of the tests pass.

# Challenges Faced:

Some of the challenges we faced include:

1) We were getting getting errors for the following Predicate Operations:
   - LT: Lesser Than
   - GT: Greater Than
   

2) We also got a Numerical Error due to invalid type conversion from double to int while using bucketWidth

# Design descisions:
1. We explicitly handled cases where the value v is outside the [min, max] range.
2. We converted bucketWidth from integer to double.

Arun used aggregate functions for 
# Time spent
Ross spent 2 hours on ColumnStats constructor, addValue() and writeup.md.
Arun spent 2 hours on estimateCardinality().

# Collaboration
Arun Ramana Balasubramaniam (BU ID: U93836083)
Ross Mikulskis (BU ID: U25561029)
