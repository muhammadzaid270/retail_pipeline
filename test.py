from fuzzywuzzy import fuzz, process

x = fuzz.ratio("this is a test", "this is a test!")
print(x)