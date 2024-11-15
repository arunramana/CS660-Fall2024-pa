# Design decisions

# Missing/incomplete elements of code
All of the code is complete; 100% of the tests pass.

# Challenges Faced:

Some of the challenges we faced include:

1) Join function; we had to get rid of duplicates on the common column only if the predicate was equality, this was not immediately intuitive.
2) Writing proper lambdas in the aggregate function, these are a little bit more
difficult to construct.
3) Understanding that we need to differentiate between int\_sum and double\_sum for aggregate since we have to
keep the tuple constructs consistent.

# Design descisions:
We used an evaluatePredicate helper to clean up the code as this logic is used throughout the functions we had to implement.

We also used a to_double helper to convert a field to double. This was used in our lambdas, e.g.:

```cpp
result = *std::max_element(values.begin(), values.end(),
	[](const field_t &a, const field_t &b) {
	return to_double(a) < to_double(b); });
```

Arun used aggregate functions for 
# Time spent
Ross spent 1.5 hours on join and writeup.md.
Arun spent 3 hours on aggregate, projection, and filter.

# Collaboration
Arun Ramana Balasubramaniam (BU ID: U93836083)
Ross Mikulskis (BU ID: U25561029)
