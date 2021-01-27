# Gianni2437-p0

This project takes in multiple text documents and calculates word counts and TF-IDF scores for key words within the documents. File paths to the documents and stopwords directories will need to be entered through the command line using the '-d' and '-s' commands. The project initially calculates raw input word counts (sp1), followed by word frequencies without specified stopwords (sp2), word frequenices with parsing for punctuation (sp3), and finally TFF-IDF calculations for each relevant word in each document (sp4). sp1, sp2, and sp3 json files contain the top 40 words from the entire corpus while sp4 output contains the combination of the top 5 words from each input document.

Problems:
- sp1 and sp2 got imperfect scores, the parsing did not remove empty strings from which ended up in the JSON output
- imperfect preprocessing also led to lower scores on sp3 and sp4
- sp4 output includes terms such as "don\u2019t"
