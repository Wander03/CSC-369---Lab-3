For all three problems, I chose to do a map-side join rather than a reduce-side join. This is because I attacked the
problem by thinking that I need to replace the addresses with the name of the country they are in. A reduce-side join
would not let me immediately do this, however a map-side join does. Furthermore, I did not need the "free sort" the
reduce-side join provides and the hostname file is small enough to fit into memory easily.

Problem 1:
CountryCount takes in the Apache HTTP access log (and uses the hostname CSV file) to count the amount of time each
country appears in the log. The output is sorted by the country name, so KeyValueSwap takes in this output and swaps
the key (country name) and value (count), implementing a custom comparator class which sorts in descending order.

Problem 2:
URLCount takes in the Apache HTTP access log (and uses the hostname CSV file) to count the number of times a url appears
for each country. They key is a Text object of the country name then the url, while the value will be the count.
Since we want to sort by country name then value descending, AdvancedSorting takes in this output and does so by
adding the value onto the end of the key (country then url then count), and using a custom comparator class to do the
specified sort. Also, the value is an empty Text object (aka nothing).

Problem 3:
CountryList takes in the Apache HTTP access log (and uses the hostname CSV file) to list all the countries which visited
each url. This time, the url is the key and the country is the value. This way, in the reduce statement all the same
urls get the countries combined into one long Text object. To avoid duplicates, the values are added to a Set (does not
allow duplicate entries), then transferred to an ArrayList to be sorted.
