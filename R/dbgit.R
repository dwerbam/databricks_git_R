#if (!require("pacman")) install.packages("pacman")
#pacman::p_load(package1, package2, package_n)
#if (!require("processx")) install.packages("processx")

library(processx)
library(fs)
library(utils)
library(shiny)
library(miniUI)

dbgit_wksp_file <- ".databricks"

cli_install <- function() {
    run("pip", args=c("install", "--upgrade", "databricks-cli"))
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

#' Initialize dbgit reposiory
#'
#' This function allow to configure a folder to be used as sync repository with databricks
#'
#' @param wksp_folder indicates the absolute location of databricks workspace e.g. /Users/some@email.com for pointing to a home folder
#'
#' export

dbgit_init <- function(wksp_folder) {
    print(wksp_folder)
    out <- run("git", args = c("init"))
    write(wksp_folder, dbgit_wksp_file)
    out <- run("git", args = c("add", dbgit_wksp_file))
    out <- run("git", args = c("commit", "-m", "'initial commit'"))
    print(paste("dbgit_init executed for workspace location",wksp_folder))
}

#' Initialize dbgit reposiory using rstudio addins
#'
#' This function allow to configure a folder to be used as sync repository with databricks, asking from the ui the specific path
#'
#' export

dbgit_init_ui <- function() {
    ui <- miniPage(
        gadgetTitleBar("dbgit init"),
        miniContentPanel(
            textInput("wksp","Databricks workspace path:")
        )
    )
    server <- function(input, output) {
        observeEvent(input$done, {
            dbgit_init(input$wksp)
            stopApp()
        })
        observeEvent(input$cancel, {
            stopApp(stop("No workspace specified.", call. = FALSE))
        })
    }
    runGadget(ui, server, viewer=dialogViewer("Workspace",height = 200))
}
#dbgit_init_ui()

#' Pull a directory from databricks
#'
#' This function allow you to sync pulling a folder FROM databricks TO local git folder
#'
#' export
#'
dbgit_pull <- function() {
    cli_install()
    env <- get_env()
    out <- run("git", args = c("status" ,"--porcelain"), echo = T, echo_cmd = T)
    if(nchar(out$stdout)>0) {
        stop("Local changes found! Please commit or stash the files first to avoid losing data.")
    }

    out <- run("databricks",
               args = c("workspace" ,
                        "export_dir",
                        "-o",
                        file.path(env$dbbase, env$relfolder),
                        file.path(env$base, env$relfolder)
               ),
               spinner = T,
               echo = T,
               echo_cmd = T
    )

}

#dbgit_init('/DEV')
#dbgit_pull()

