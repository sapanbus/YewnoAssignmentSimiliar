# YewnoAssignmentSimiliar
Implement Jaccardian similarity as outlined below or implement and explain the usage of a different similarity metric.  

Input data set: List of text documents 
Rough Algorithm outline:

1. Decide on some frame where words must co-occur to be counted as a pair; i.e.: whole document, paragraph, sentence. 
2. Calculate jaccard similarity as |X intersect Y|/|X union Y| where X and Y are individual words and the magnitude is the number of frames where the condition is true.  

Output data set: Similarity of all pairs of words contained in the text documents 
