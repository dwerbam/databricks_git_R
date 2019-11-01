#if (!require("pacman")) install.packages("pacman")
#pacman::p_load(package1, package2, package_n)
#if (!require("processx")) install.packages("processx")

library(processx)
library(fs)
library(utils)
library(dplyr)

dbgit_wksp_file <- ".databricks"

remove_empty <- function(x) return( x[x!="" ])
runit <- function(cmd, args) {
    return(
        run(cmd,
            args = remove_empty(args),
            spinner = T,
            echo = T,
            echo_cmd = T
        ))
}

cli_install <- function() {
    runit("pip", args=c("install", "--upgrade", "databricks-cli"))

    cfgpath=file.path(Sys.getenv("HOME"),'.databrickscfg')
    if(!file.exists(cfgpath)) stop(paste('Cannot find databricks configuration file in ', cfgpath))

}
#cli_install()

path_up <- function(p) {
    if(p=="/") { return("/") }
    return(path_join(head(unlist(path_split(p)),-1)))
}

find_up <- function(pwd, name) {
    n <- file.path(pwd, name)
    there <- file.exists(n)
    if( !there & pwd=="/" ) { return(NA) }
    if( there ) { return(paste0(pwd)) }
    return(find_up(path_up(pwd), name)) # not there yet, recurse
}

get_env <- function() {
    pwd <- getwd()
    base <- find_up(pwd, dbgit_wksp_file)
    if(is.na(base)) {
        stop("Databricks folder not found")
    }

    dbbase=readLines(file.path(base,dbgit_wksp_file))
    # check output?

    return(list(
        pwd = pwd,
        base = base,
        relfolder = substring(pwd, nchar(base, allowNA = T)+1),
        dbbase = dbbase
    ))
}

pjoin <- function(...) return(gsub('//', '/', file.path(...)))

#' Initialize dbgit reposiory
#'
#' This function allow to configure a folder to be used as sync repository with databricks
#'
#' @param wksp_folder indicates the absolute location of databricks workspace e.g. /Users/some@email.com for pointing to a home folder
#'
#' export

dbgit_init <- function(wksp_folder="") {
    if(wksp_folder=="") wksp_folder <- readline("Please, enter databricks workspace folder:")
    print(wksp_folder)

    gitpath=file.path(Sys.getenv("HOME"),'.gitconfig')
    if(!file.exists(gitpath)) stop(paste('Cannot find GIT configuration file in ', gitpath))

    out <- runit("git", args = c("init"))
    write(wksp_folder, dbgit_wksp_file)
    out <- runit("git", args = c("add", dbgit_wksp_file))
    out <- runit("git", args = c("commit", "-m", "'initial commit'"))
    print(paste("dbgit_init executed for workspace location",wksp_folder))
}


#' Pull a directory from databricks
#'
#' This function allow you to sync pulling a folder FROM databricks TO local git folder
#'
#' export
#'
dbgit_pull <- function() {
    cli_install()
    env <- get_env()
    out <- runit("git", args = c("status" ,"--porcelain"))
    if(nchar(out$stdout)>0) {
        stop("Local changes found! Please commit or stash the files first to avoid losing data.")
    }

    out <- runit("databricks",
               args = c("workspace" ,
                        "export_dir",
                        "-o",
                        file.path(env$dbbase, env$relfolder),
                        file.path(env$base, env$relfolder)
               ))

}

#' Push a directory to databricks
#'
#' This function allow you to sync pushing a local git folder TO databricks
#'
#' export
#'
dbgit_push <- function(overwrite=FALSE) {
    cli_install()
    env <- get_env()
    print("Pushing directory to databricks..")

    writemode=ifelse(overwrite, "-o", "")
    out <- runit("databricks",
               args = c("workspace" ,
                        "import_dir",
                        writemode,
                        "-e",
                        file.path(env$base, env$relfolder),
                        file.path(env$dbbase, env$relfolder)
               ))

}

#' Push a file to databricks
#'
#' This function allow you to sync pushing a local git file TO databricks
#'
#' export
#'
dbgit_pushfile <- function(filename, overwrite=FALSE) {
    cli_install()
    env <- get_env()
    print("Pushing file to databricks..")
    writemode=ifelse(overwrite, "-o", "")

    fileonly <- path_ext_remove(filename)
    lowerfile <- tolower(filename)
    language = case_when(
        endsWith(lowerfile, ".py") ~ "PYTHON",
        endsWith(lowerfile, ".scala") ~ "SCALA",
        endsWith(lowerfile, ".r") ~ "R",
        endsWith(lowerfile, ".sql") ~ "SQL",
        TRUE ~ "unknown"
    )

    out <- runit("databricks",
               args = remove_empty(
                   c("workspace" ,
                        "import",
                        "-l",
                        language,
                        writemode,
                        pjoin(env$base, env$relfolder, filename),
                        pjoin(env$dbbase, env$relfolder, fileonly))

               ))

}

#dbgit_init('/DEV')
#dbgit_pull()

