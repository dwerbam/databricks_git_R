% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dbgit.R
\name{dbgit_pushfile}
\alias{dbgit_pushfile}
\title{Push a file to databricks}
\usage{
dbgit_pushfile(filename, overwrite = FALSE)
}
\arguments{
\item{filename}{indicating the filename to upload to databricks.}

\item{overwrite}{is a boolean indicating if we should overwrite the file if already exists. By default is FALSE.}
}
\description{
This function allow you to sync pushing a local git file TO databricks
}
