category <- c("positive", "negative", "uncertainty",
                "litigious", "modal_strong", "modal_weak")

base_url <- "http://www3.nd.edu/~mcdonald/Data/Finance_Word_Lists"

url <- file.path(base_url,
                 c("LoughranMcDonald_Positive.csv",
                   "LoughranMcDonald_Negative.csv",
                   "LoughranMcDonald_Uncertainty.csv",
                   "LoughranMcDonald_Litigious.csv",
                   "LoughranMcDonald_ModalStrong.csv",
                   "LoughranMcDonald_ModalWeak.csv"))

df <- data.frame(category, url, stringsAsFactors=FALSE)

getWords <- function(url) {
    words <- read.csv(url, as.is=TRUE)
    paste(words[,1], collapse=",")
}
df$words <- unlist(lapply(df$url, getWords))

library(readr)
write_csv(df, path = "lm_words.csv")