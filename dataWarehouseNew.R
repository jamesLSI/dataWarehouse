library(RODBC)
library(tidyverse)
source("dwCreds.R")

dataWarehouseFunction <- function(){
  
  source("dwCreds.R",
         chdir = T)
  
  con <- DBI::dbConnect(drv      = RMySQL::MySQL(), 
                        username = usernameDw, 
                        password = passwordDw, 
                        host     = "dw-prod.cluster-csfwpi01op01.eu-west-2.rds.amazonaws.com", 
                        port     = 3306, 
                        dbname   = "dw")

  tables <- tibble(name = c("Applications",
                            "Projects",
                            "Monitoring",
                            "Competitions"))

  choice <- menu(tables$name)
  
  tableChoice <- tables$name[choice]
  
  start <- Sys.time()
  
  if (tableChoice == "Applications") {
    
    print("This could take up to 6 minutes")
    
    
    applicantTable <- DBI::dbReadTable(con, "vw_Dashboard_IFSApplicant")
    
    applicationTable <- DBI::dbReadTable(con, "vw_Dashboard_IFSApplication")
    
    output <- applicationTable %>% 
      left_join(applicantTable) %>% 
      mutate(ApplicantKey = as.character(ApplicantKey),
             ApplicationID = as.character(ApplicationID))
    
    } else if (tableChoice == "Projects") {
      
      print("This could take up to 2 minutes")
      
      projectTable <- DBI::dbReadTable(con, "vw_Dashboard_IFSPA_Project")       
      
      participantTable <- DBI::dbReadTable(con, "vw_Dashboard_IFSPA_ProjectParticipant")
      
      output <- projectTable %>% 
        select(-IsOkToPublish) %>% 
        left_join(participantTable,
                  by = "ProjectNumber") %>% 
        select(-contains(".y"))
      
      names(output) <- gsub("\\.x", "", names(output))

    } else if (tableChoice == "Monitoring") {
      
      print("This could take up to 2 minutes")
      
      output <- DBI::dbReadTable(con, "vw_Dashboard_IFSPA_MonitoringData")
      
    } else if (tableChoice == "Competitions") {
      
      print("This could take up to a minute")
      
      comps <- DBI::dbReadTable(con, "vw_Dashboard_IFSCompetition")
      compsFunding <- DBI::dbReadTable(con, "vw_Dashboard_IFSCompetitionFunder")
      compsMilestones <- DBI::dbReadTable(con, "vw_Dashboard_IFSCompetitionMilestone")
      
      output <- compsMilestones %>% 
        select(CompetitionKey,
               localDate,
               type) %>% 
        pivot_wider(names_from = type,
                    values_from = localDate) %>% 
        left_join(comps,
                  by = "CompetitionKey") %>%
        left_join(compsFunding,
                  Joining, by = c("CompetitionKey", "CompetitionID", "CompetitionName"))
      }
      
  end <- Sys.time()
  print(end-start)
  
  rm(usernameDw,
     passwordDw,
     inherits = T)
  
  return(output)
  
}
