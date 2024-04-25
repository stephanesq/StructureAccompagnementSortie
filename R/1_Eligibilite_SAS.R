# Outputs
# OBJECTIFS
#   - Déterminer quand éligible à un aménagement
#   - + éligibile à une LSC et LSC-D
#   
# ORGANISATION
# - Rajouter informations sur :
#   - Date écrou initial
#   - Date de libération + motif si LC
# 
# - Modifier situ pénale
#   - Conserver que CON ou CP
#   - Renormalisation : 
#       Une date de situation plutôt que date de début = date de fin  + 1
#   - situ pénale avant libération OU si LC avant date de libération prévisionnelle
#   
# - Eligibilite 
#   - Rappel des critères
#     1. AP : <5 ans total et <2 ans avant la fin prev
#     2. LSC : <5 ans total et <1/3 avant la fin prev
#     3. LSC-D : <2 ans total et <3 mois avant la fin prev
#   - Traitement dans les données
#     1. AP :  eligib < date situ pénale suivante OU date libération prév
#   
# - On fait un premier traitement pour déterminer les périodes pendant lesquelles la personne est éligible à la LSC en fonction du QTM, de la catégorie pénale et des 2/3 de la date de libération prev
# - On réunit ces périodes, peu importe la date des 2/3 elle-même


# paramètres ---------
# packages ----
pacman::p_load(tidyverse, arrow, data.table, janitor, haven, styler,data.table, ggsurvfit)
# chemin ------
path = paste0(here::here(),"/Donnees/")
path_dwh = "~/Documents/Recherche/3_Evaluation/_DATA/INFPENIT/"
path_ref = "~/Documents/Recherche/3_Evaluation/_DATA/Referentiel/"

# 1. Import des bases de travail ----
## 1.1. Situ pénale ------

# Traitement :
#   - Date de libération prévisionnelle 
#     - Problème de déperdition info date de lib prévisionnelle avant 2016
#     - Que si date de libération prévisionnelle DISPO et si < DT_DEBUT_SITU_PENALE -> mais que après renormalisation (sinon des lignes ne veulent plus rien dire)
#   - Redressements utiles sur : date, format QTM et Catégorie pénale
#   - Mais utilisation IP sans filtre source = pb -> 6 x plus de lignes

# A vérifier :
#   1. Quelle est la date de lib prev quand on est prévenu ?
#   2. Pourquoi des cas comme 1015280006347


t_dwh_h_situ_penale <- open_dataset(paste0(path_dwh,"t_dwh_h_situ_penale.parquet"))  |>
  # Définir la requête
    # Filtre et selection
    filter(TP_SOURCE == "GENESIS") |>
    select(NM_ECROU_INIT, DT_DEBUT_SITU_PENALE, DT_FIN_SITU_PENALE, 
           CD_CP_GLOBALE, CD_CP_GLO_MAN, CD_CP_DETAIL, DT_LIBE_PREV, 
           QTM_FERME_TACC_A, QTM_FERME_TACC_M, QTM_FERME_TACC_S, QTM_FERME_TACC_J) |>
    collect() |>
  
  # changement des formats
  mutate(
    across(starts_with("DT_"), as.Date),
    across(starts_with("QTM_"), as.numeric)
    ) |>
    # Quantum ferme
    mutate(QTM_FERME_TACC = QTM_FERME_TACC_A * 360 + QTM_FERME_TACC_M * 30 + QTM_FERME_TACC_S * 7 + QTM_FERME_TACC_J) |>
    select(-QTM_FERME_TACC_A, -QTM_FERME_TACC_M, -QTM_FERME_TACC_S, -QTM_FERME_TACC_J)  |>
    # Tri pour la suite
    arrange(NM_ECROU_INIT, DT_DEBUT_SITU_PENALE)  |>
  
  group_by(NM_ECROU_INIT) |>
    # garder date fin en date début
    mutate(DT_SITU_PENALE = lag(DT_FIN_SITU_PENALE, default = first(DT_DEBUT_SITU_PENALE, na.rm = TRUE))) |>
  ungroup() |>
  # on enlève les dates inutiles
  select(-DT_FIN_SITU_PENALE, -DT_DEBUT_SITU_PENALE) |>
  # on garde que ligne exploitable (situ < lib_prev et lib_prev dispo)
  filter(DT_SITU_PENALE <= DT_LIBE_PREV & !is.na(DT_LIBE_PREV)) |>
  
  # Catégorie pénale (à partir de la seule situ_penale) 
  mutate(
    CD_CP_REDR = as.factor(
      case_when(
        ! CD_CP_GLOBALE %in% c('(ND)', '-', '') ~ as.character(CD_CP_GLOBALE),
        ! CD_CP_GLO_MAN %in% c('(ND)', '-', '') ~ as.character(CD_CP_GLO_MAN),
        CD_CP_DETAIL %in% c("PR", "PRE", "PRV", "APP", "DAP", "DPO", "OPP", "POU") ~ "PR",
        .default = "CO"))) |>
  select(-CD_CP_GLOBALE, -CD_CP_GLO_MAN, -CD_CP_DETAIL)


gc()
## Date max 
DT_SITU_PENALE_MAX <- max(t_dwh_h_situ_penale$DT_SITU_PENALE)


## 1.2. Date d'écrou initial  ------
t_dwh_ecrou_init <- open_dataset(paste0(path_dwh,"t_dwh_ecrou_init.parquet"))

t_dwh_ecrou_init <- t_dwh_ecrou_init |>
  select(NM_ECROU_INIT, DT_ECROU_INITIAL) |>
  mutate(DT_ECROU_INITIAL=as.Date(DT_ECROU_INITIAL)) |>
  collect()

t_dwh_ecrou_init <- t_dwh_ecrou_init %>% 
  distinct() 

## 1.3 Date de libération ----

t_dwh_f_mouvement <- open_dataset(paste0(path_dwh,"t_dwh_f_mouvement.parquet"))

t_dwh_f_mouvement <- t_dwh_f_mouvement|>
  filter(TP_SOURCE ==  "GENESIS") |>
  filter(CD_TYPE_MOUVEMENT == "LIB" & CD_NATURE_MOUVEMENT == "LEVECR") |>
  select(NM_ECROU_INIT, DT_MOUVEMENT_REEL,CD_MOTIF_MOUVEMENT) |>
  collect() |>

  group_by(NM_ECROU_INIT) |>
  mutate(DT_LEVEECR = max(DT_MOUVEMENT_REEL)) |>
  ungroup()  |>
  filter(DT_LEVEECR==DT_MOUVEMENT_REEL) |> 
  mutate(DT_LEVEECR=as.Date(DT_LEVEECR),
         LEVEECR_LC=if_else(str_detect(CD_MOTIF_MOUVEMENT, "^LC") & CD_MOTIF_MOUVEMENT != 'LCTRT',1,0)) |>
  select(-DT_MOUVEMENT_REEL)

gc()

#2. Croiser information  ----

##2.1 Levée écrou et LC --------------
h_situ_penale <- t_dwh_h_situ_penale %>% 
  left_join(t_dwh_f_mouvement) 
# nettoyage
rm(t_dwh_f_mouvement,t_dwh_h_situ_penale)
##2.2. Ecrou init / Elibilite LSC --------------
# Passage en data.table
# eligibilité en comparant date de mise à l'écrou et date de lib prev
h_situ_penale <- data.table(h_situ_penale)
t_dwh_ecrou_init <- data.table(t_dwh_ecrou_init)

# Indexer les tables
# set the ON clause as keys of the tables:
setkey(h_situ_penale,NM_ECROU_INIT)
setkey(t_dwh_ecrou_init,NM_ECROU_INIT)

# Inner join avec date écrou initial nomatch=0 -> autre possibilité merge() de data.table
h_situ_penale <- h_situ_penale[t_dwh_ecrou_init, nomatch = 0]
# nettoyage
rm(t_dwh_ecrou_init)

# Calcul 2/3 de peine + date de fin de peine
h_situ_penale[, `:=`(
  DT_DEUXTIERSDEPEINE = floor_date(
    time_length(difftime(DT_LIBE_PREV, DT_ECROU_INITIAL), "days") * 2/3 + DT_ECROU_INITIAL,
    unit = "day"
  ),
  DT_DEUX_ANS_AVT = DT_LIBE_PREV - years(2),
  DT_TROIS_MOIS_AVT = DT_LIBE_PREV - months(3),
  DT_FIN_PEINE = if_else(LEVEECR_LC == 1, DT_LIBE_PREV,DT_LEVEECR) #date de libération prévisionnelle si sortie pour LC sinon 
)]

# Récupére prochaine situ pénale (shift) ou date lib prev si absente (fcoalesce)
h_situ_penale <- h_situ_penale[order(NM_ECROU_INIT, DT_SITU_PENALE)]
h_situ_penale[, `:=`(
  DT_NEXT_SITU_PENALE = fcoalesce(
    shift(DT_SITU_PENALE, type = "lead"),
    DT_FIN_PEINE)
  ), 
  by = NM_ECROU_INIT]

# calcul éligibilité
h_situ_penale[, `:=`(
    ELIGIBLE_AP = fifelse(
      QTM_FERME_TACC > 0 &
        QTM_FERME_TACC/360 <= 5 & 
        DT_DEUX_ANS_AVT <= DT_NEXT_SITU_PENALE
    , 1L, 0L),
  ELIGIBLE_LSC = fifelse(
    QTM_FERME_TACC > 0 &
      QTM_FERME_TACC/360 <= 5 & 
      DT_DEUX_ANS_AVT <= DT_NEXT_SITU_PENALE
    , 1L, 0L),
  ELIGIBLE_LSCD = fifelse(
    QTM_FERME_TACC > 0 &
      QTM_FERME_TACC/360 <= 2 & 
      DT_TROIS_MOIS_AVT <= DT_NEXT_SITU_PENALE ,1L, 0L) #les parties 1L et 0L correspondent à des entiers littéraux en R, pour éviter que R croit à des décimaux ou autres
  )]


#3. Eligibilité AP/LSC/LSC-D ---------
##3.1. ELIG_AP -------
## Période d'éligibilité à un aménagement de peine (unique par NM_ECROU_INIT)
### Créer une colonne pour les groupes consécutifs de ELIG == 1 (séparés par des lignes ELIG =0)
### cumsum(ELIG==0) crée un groupe cumulatif (1, 1, 2, 2, 2, 2, 1, 2, 2, 2, 2) basé sur les indices où ELIG est égal à 0
eligible_ap <- h_situ_penale[, group := cumsum(ELIGIBLE_AP==0)]
## rleid() crée un identifiant unique pour chaque groupe distinct par ID
eligible_ap[, group := rleid(group), by=NM_ECROU_INIT]
## supprime
eligible_ap <-eligible_ap[ELIGIBLE_AP==1,]
## Dates début et fin
### 
eligible_ap <- eligible_ap[order(NM_ECROU_INIT, DT_SITU_PENALE, group)]
setkeyv(eligible_ap,c("NM_ECROU_INIT","group"))
### 
eligible_ap[, `:=`(
  DT_DBT_ELIG = first(DT_SITU_PENALE),
  DT_FIN_ELIG = last(DT_NEXT_SITU_PENALE))
            , by=.(NM_ECROU_INIT,group)]
### Garde qu'une ligne
eligible_ap <- eligible_ap[, .SD[1], by=.(NM_ECROU_INIT,group)] # extract first row of groups.
### créé un compteur d'éligibilité
eligible_ap <- eligible_ap[ ,  `:=`( 
  group = seq(.N),
  nb_group = .N,
  annee_dbt_elig_ap = year(DT_DBT_ELIG))
  , by = .(NM_ECROU_INIT)]
# table(eligible_ap$group)
# 1      2 
# 660756      1
### Qu'écrou avec un seul SPELL d'éligibilité
eligible_ap <- eligible_ap[nb_group == 1, ]
### Enleve les colonnes inutiles
eligible_ap <- eligible_ap[, `:=`(
  group = NULL,
  nb_group = NULL,
  DT_SITU_PENALE = NULL,
  DT_NEXT_SITU_PENALE = NULL,
  ELIGIBLE_AP = NULL
)]
# nettoyage
rm(h_situ_penale)
## 3.2 ELIG LSC(-D) ------
## Pour LSC, dépend uniquement de l'éligibilité et de la date
## Pour LSC-D, manque d'autres infos, pas dispo à ce stade
eligible_ap <- eligible_ap[, `:=`(
  DT_DBT_ELIG_LSC = fifelse(
    ELIGIBLE_LSC == 1 & DT_DEUXTIERSDEPEINE <= DT_FIN_ELIG, 
    pmax(DT_DEUXTIERSDEPEINE, DT_DBT_ELIG), # pour éviter de prendre une date avant le début de l'éligibilité à l'AP
    NA_Date_), #vide sinon
  DT_DBT_ELIG_LSCD = fifelse(
    ELIGIBLE_LSCD == 1 & DT_TROIS_MOIS_AVT <= DT_FIN_ELIG, 
    pmax(DT_TROIS_MOIS_AVT, DT_DBT_ELIG), 
    NA_Date_),
  DT_DEUXTIERSDEPEINE = NULL,
  DT_TROIS_MOIS_AVT = NULL,
  ELIGIBLE_LSC = NULL,
  ELIGIBLE_LSCD = NULL
)]
## 3.3 Filtre et export -----
## Filtres : 
##  - pas de date de fin postérieure à la date de chargement des données
##  - pas d'éligibilité avant 2016 (compris)
eligible_ap <- eligible_ap[DT_FIN_ELIG < DT_SITU_PENALE_MAX &
                             annee_dbt_elig_ap > 2016
                           , ]
## Export
write_parquet(eligible_ap,paste0(path,"Export/eligible_ap.parquet"))

# 4. Aménagement de peine ----
### AP dans situ pénit
### On réduit aux NM_ECROU_INIT pertinents
### TOP_ECROUE ==1 : supprime les intervalles avec une libération (TOP_SORTIE_DEF ==0) et plus généralement les pas utiles 

## 4.1 Import -----
## Redressement avec les règles de gestion
t_dwh_h_situ_penit <- open_dataset(paste0(path_dwh,"t_dwh_h_situ_penit.parquet")) |> 
  filter(TOP_ECROUE == 1) |>  
  select(NM_ECROU_INIT,
       DT_DEBUT_SITU_PENIT,DT_FIN_SITU_PENIT,
       CD_MOTIF_HEBERGEMENT,TOP_HEBERGE,CD_STATUT_SEMI_LIBERTE, CD_CATEG_ADMIN,
       TOP_LSC, CD_TYPE_AMENAGEMENT, CD_AMENAGEMENT_PEINE, 
       DT_DEBUT_EXEC,DT_SUSPSL) |>
  # Redressement aménagement de peine
  mutate(top_detention = if_else(CD_MOTIF_HEBERGEMENT %in% c('PE','PSEM','PSE','SEFIP','DDSE'),0,1),    
         AMENAGEMENT = case_when(
           top_detention==0 & CD_MOTIF_HEBERGEMENT %in% c('PSE', 'PSEM', 'SEFIP') ~ "DDSE", #PSE
           top_detention==0 & CD_MOTIF_HEBERGEMENT == 'DDSE'  ~ "DDSE",
           top_detention==0 & CD_MOTIF_HEBERGEMENT == "PE" ~ "PE_nheb",
           top_detention==0 & CD_TYPE_AMENAGEMENT %in% c('PSE', 'PSEM', 'SEFIP') ~ "DDSE", #PSE
           top_detention==0 & CD_TYPE_AMENAGEMENT == 'DDSE' ~ "DDSE",
           top_detention==0 & CD_TYPE_AMENAGEMENT == 'PE' ~ "PE_nheb",
           top_detention==1 & CD_AMENAGEMENT_PEINE == "PE" ~ "PE_heb",
           top_detention==1 & CD_AMENAGEMENT_PEINE == "SL" & (CD_STATUT_SEMI_LIBERTE == "O" | str_detect(CD_CATEG_ADMIN, "SL")) ~ "SL",
           top_detention==1 & CD_TYPE_AMENAGEMENT == "SL" & (CD_STATUT_SEMI_LIBERTE == "O" | str_detect(CD_CATEG_ADMIN, "SL")) ~ "SL",
           .default = NA_character_
         )) %>% 
  # Pas possible de filtrer maintenant à cause des problèmes de décalage de date des début de situ penit
  # filter(!is.na(AMENAGEMENT)) |> 
  select(-top_detention, -CD_MOTIF_HEBERGEMENT, -CD_TYPE_AMENAGEMENT, -CD_AMENAGEMENT_PEINE,-CD_STATUT_SEMI_LIBERTE,-CD_CATEG_ADMIN,-TOP_HEBERGE) |>
  collect() 
# table(T_DWH_H_SITU_PENIT$TOP_ECROUE,T_DWH_H_SITU_PENIT$TOP_SORTIE_DEF)

## 4.2 Modification ------
## Récupérer les écrous de la table précédente
h_situ_penit <- open_dataset(paste0(path,"Export/eligible_ap.parquet")) |>  
  select(NM_ECROU_INIT) |> 
  collect() |> 
    inner_join(t_dwh_h_situ_penit) |> 
    distinct() 
## Enlève t_dwh
rm(t_dwh_h_situ_penit)
### Passage en data.table
h_situ_penit <- data.table(h_situ_penit)
setkey(h_situ_penit,NM_ECROU_INIT)

### 4.2.2. Changement organisation table (data.table) -----
# Décalage puis que aménagement et que bonne date
# fill using first : https://rdatatable.gitlab.io/data.table/reference/shift.html
h_situ_penit <- h_situ_penit[order(NM_ECROU_INIT, DT_DEBUT_SITU_PENIT)]
h_situ_penit[ , DT_SITU_PENIT := 
                as.Date(
                  shift(DT_FIN_SITU_PENIT, 
                        fill=DT_DEBUT_SITU_PENIT[1L], 
                        n=1, 
                        type="lag")
                )
            , by=NM_ECROU_INIT]
# supprime les intervalles
h_situ_penit <- h_situ_penit[,`:=`(
  DT_DEBUT_SITU_PENIT=NULL,
  DT_FIN_SITU_PENIT=NULL,
  DT_DEBUT_EXEC = as.Date(DT_DEBUT_EXEC),
  DT_SUSPSL = as.Date(DT_SUSPSL)
)]
# dédoublonne et tri
h_situ_penit <- unique(h_situ_penit)
h_situ_penit <- h_situ_penit[order(NM_ECROU_INIT, DT_SITU_PENIT)]
# identifier les périodes avec aménagement
# utiliser les DT_DEBUT_EXEC et les DT_SUSPSL pour modifier ces périodes
h_situ_penit <- h_situ_penit[,`:=`(
  DT_SITU_PENIT = fifelse(
    !is.na(DT_DEBUT_EXEC) & !is.na(AMENAGEMENT) & #date et aménagement renseigné
      DT_DEBUT_EXEC>DT_SITU_PENIT & 
      ( DT_DEBUT_EXEC <= shift(DT_SITU_PENIT, n=1, type="lead") | 
        is.na(shift(DT_SITU_PENIT, n=1, type="lead"))
      ) #si la date de début d'exécution commence avant la ligne suivante ou qu'il n'y en a pas
    ,DT_DEBUT_EXEC
    ,DT_SITU_PENIT
  ),
  AMENAGEMENT = fifelse(
    !is.na(DT_DEBUT_EXEC) & !is.na(AMENAGEMENT) & #date et aménagement renseigné
      DT_DEBUT_EXEC>DT_SITU_PENIT & 
      ( DT_DEBUT_EXEC <= shift(DT_SITU_PENIT, n=1, type="lead") | 
          is.na(shift(DT_SITU_PENIT, n=1, type="lead"))
      ) #si la date de début d'exécution commence avant la ligne suivante ou qu'il n'y en a pas
    , NA_character_
    , AMENAGEMENT
    ))]
# pareil avec suspension 
h_situ_penit <- h_situ_penit[,`:=`(
  DT_SITU_PENIT = fifelse(
    !is.na(DT_SUSPSL) & !is.na(AMENAGEMENT) & year(DT_SUSPSL)>1900 & #date et aménagement renseigné
      DT_SUSPSL>=DT_SITU_PENIT & (
        DT_SUSPSL <= shift(DT_SITU_PENIT, n=1, type="lead") | 
        is.na(shift(DT_SITU_PENIT, n=1, type="lead"))
              )
    ,DT_SUSPSL
    ,DT_SITU_PENIT
  ),
  AMENAGEMENT = fifelse(
      !is.na(DT_SUSPSL) & !is.na(AMENAGEMENT) & year(DT_SUSPSL)>1900 & #date et aménagement renseigné
        DT_SUSPSL>=DT_SITU_PENIT & (
          DT_SUSPSL <= shift(DT_SITU_PENIT, n=1, type="lead") | 
            is.na(shift(DT_SITU_PENIT, n=1, type="lead"))
        )
      ,NA_character_
      ,AMENAGEMENT
    ),
  INDIC_AMENAGEMENT = fifelse(is.na(AMENAGEMENT),0,1)
  )]
# supprime les intervalles
h_situ_penit <- h_situ_penit[,`:=`(
  DT_DEBUT_EXEC=NULL,
  DT_SUSPSL=NULL
)]
## 4.3 Phase d'AP ------
### 4.3.1 Date suivante ou levée d'écrou ------
# libération -> data.table
f_levecr <- open_dataset(paste0(path_dwh,"t_dwh_f_mouvement.parquet")) |>
  filter(TP_SOURCE ==  "GENESIS") |>
  filter(CD_TYPE_MOUVEMENT == "LIB" & CD_NATURE_MOUVEMENT == "LEVECR") |>
  select(NM_ECROU_INIT, DT_MOUVEMENT_REEL) |>
    group_by(NM_ECROU_INIT) |>
    summarise(DT_LEVEECR = max(DT_MOUVEMENT_REEL)) |>
  collect() 
### Passage en data.table
f_levecr <- data.table(f_levecr)
setkey(f_levecr,NM_ECROU_INIT)  
### Récupérer l'info puis supprile
h_situ_penit <- merge(h_situ_penit,f_levecr)
rm(f_levecr)
# Date suivante/levée d'écrou pour la penit -> afin d'avoir une date de fin si besoin
h_situ_penit[, `:=`(
  DT_NEXT_SITU_PENIT = fcoalesce(
    shift(DT_SITU_PENIT, type = "lead"),
    as.Date(DT_LEVEECR))
), by = NM_ECROU_INIT]
gc()
### 4.2.3. Phase d'AP ------
## Période d'éligibilité à un aménagement de peine (unique par NM_ECROU_INIT)
### Créer une colonne pour les groupes consécutifs de ELIG == 1 (séparés par des lignes ELIG =0)
### cumsum(ELIG==0) crée un groupe cumulatif (1, 1, 2, 2, 2, 2, 1, 2, 2, 2, 2) basé sur les indices où ELIG est égal à 0
situ_penit_ap <- h_situ_penit[, group := cumsum(INDIC_AMENAGEMENT==0)]
rm(h_situ_penit)
## rleid() crée un identifiant unique pour chaque groupe distinct par ID
situ_penit_ap[, group := rleid(group), by=NM_ECROU_INIT]
## supprime
situ_penit_ap <-situ_penit_ap[INDIC_AMENAGEMENT==1,]
## Dates début et fin
### 
situ_penit_ap <- situ_penit_ap[order(NM_ECROU_INIT, DT_SITU_PENIT, group)]
setkeyv(situ_penit_ap,c("NM_ECROU_INIT","group"))
### 
situ_penit_ap[, `:=`(
  DT_DBT_AP = first(DT_SITU_PENIT),
  DT_FIN_AP= last(DT_NEXT_SITU_PENIT))
  , by=.(NM_ECROU_INIT,group)]
### Garde qu'une ligne
situ_penit_ap <- situ_penit_ap[, .SD[1], by=.(NM_ECROU_INIT,group)] # extract first row of groups.
### créé un compteur d'éligibilité
situ_penit_ap <- situ_penit_ap[ ,  `:=`( 
  group = seq(.N),
  nb_group = .N,
  annee_dbt_ap = year(DT_DBT_AP))
  , by = .(NM_ECROU_INIT)]
# table(situ_penit_ap$group)
# 1      2      3      4      5      6      7      8      9     10 
# 241888  11521   1257    188     37     13      6      4      2      1 
### QConserve que la première phase d'AP -> pour l'instant pas besoin 
# situ_penit_ap <- situ_penit_ap[group == 1, ]
### Enleve les colonnes inutiles
DT_SITU_PENIT_MAX <- max(situ_penit_ap$DT_SITU_PENIT)
situ_penit_ap <- situ_penit_ap[, `:=`(
  DT_SITU_PENIT = NULL,
  DT_NEXT_SITU_PENIT = NULL,
  INDIC_AMENAGEMENT = NULL
)]
## 4.4 Filtre et export -----
## Filtres : 
##  - pas de date de fin postérieure à la date de chargement des données
##  - pas d'éligibilité avant 2016 (compris)
situ_penit_ap <- situ_penit_ap[DT_DBT_AP <= DT_SITU_PENIT_MAX &
                               annee_dbt_ap > 2016
                           , ]
## Export
write_parquet(situ_penit_ap,paste0(path,"Export/situ_penit_ap.parquet"))


# 5. Table finale ----
## 5.1. Import -----
suivi_ap <-  open_dataset(paste0(path,"Export/eligible_ap.parquet")) |>
  select(-annee_dbt_elig_ap, -DT_DEUX_ANS_AVT) |> 
  collect() |> 
  left_join(
    open_dataset(paste0(path,"Export/situ_penit_ap.parquet")) |> 
      select(-DT_LEVEECR, -annee_dbt_ap) |> 
      collect(),
    by = join_by(NM_ECROU_INIT,
                 closest(DT_DBT_ELIG <= DT_DBT_AP))
  ) 
suivi_ap <- data.table(suivi_ap)
### Filtre dates incohérentes (> à observation de l'autre)
DT_SITU_PENAL_MAX <- max(suivi_ap$DT_FIN_ELIG, na.rm = T) #max de la situation pénale
DT_SITU_PENIT_MAX <- max(suivi_ap$DT_DBT_AP, na.rm = T) #max de la situ penit
DT_MAX <- pmin(DT_SITU_PENIT_MAX,DT_SITU_PENALE_MAX) # la plus petite des deux
suivi_ap <- suivi_ap[DT_FIN_ELIG <= DT_MAX & 
                       (is.na(DT_DBT_AP) | DT_DBT_AP <= DT_MAX), ] #on retire les infos post

## 5.2 LC ------
### 5.2.1 avec les levées d'écrou (modif aménagement et date) ------
suivi_ap <- suivi_ap[, `:=`(
  AMENAGEMENT = fcase(
    is.na(AMENAGEMENT) & LEVEECR_LC == 1, "LC", 
    !is.na(AMENAGEMENT), AMENAGEMENT,
    default = "Non_AP")
)]
## modif date
suivi_ap <- suivi_ap[AMENAGEMENT == "LC", `:=`(
  DT_DBT_AP = DT_LEVEECR,
  DT_FIN_AP = DT_FIN_PEINE
  )]
## crée une variable générale
suivi_ap <- suivi_ap[, `:=`(
  AP = fifelse(AMENAGEMENT == "Non_AP",0,1)
)]

### 5.2.2. dans les mesures -----
## 30150 30160 30520 (REVOCATION TOTALE LC)

## 5.3 Export -----
### 5.3.1 Aménagement a la mise sous écrou + filtre ----
suivi_ap <- suivi_ap[, `:=`(
  AMENAGEMENT_MSE = fifelse(DT_DBT_AP <=  DT_ECROU_INITIAL + days(7) & !is.na(DT_DBT_AP), 1, 0),
  DT_LIBE_PREV = NULL,
  CD_MOTIF_MOUVEMENT = NULL,
  DT_LEVEECR = NULL,
  LEVEECR_LC = NULL 
)]
### 5.3.2. Parquet
write_parquet(suivi_ap,paste0(path,"Export/suivi_ap.parquet"))

# 6. Premières analyses -----
## 6.1. Analyse de survie (Larmarange) -----
## https://larmarange.github.io/analyse-R/analyse-de-survie.html
### 5.1.1. Import hors aménagement à la mise sous écrou -----
suivi_ap <-  open_dataset(paste0(path,"Export/suivi_ap.parquet")) |> 
  collect()
suivi_ap <- data.table(suivi_ap)
suivi_ap <- suivi_ap[AMENAGEMENT_MSE == 0,]
         
### 5.1.2. Variables nécessaires ------
## Durée observation
suivi_ap[, duree_observation := time_length(interval(DT_DBT_ELIG, DT_FIN_ELIG), unit = "months")]
## Durée AP + imputer pour éviter égalité avec une décimale
suivi_ap[, duree_ap := time_length(interval(DT_DBT_ELIG, DT_DBT_AP), unit = "months")]
suivi_ap[, duree_ap_impute := duree_ap + runif(.N)]
## Temps
suivi_ap[, time := duree_observation]
suivi_ap[AP == 1, time := duree_ap_impute]

## 5.2 Kapman Meier -----
# https://www.danieldsjoberg.com/ggsurvfit/
# KM
p <- ggsurvfit::survfit2(Surv(time, AP) ~ year(DT_DBT_ELIG), data = suivi_ap) |>
  ggsurvfit(linewidth = 1) +
  add_confidence_interval() +
  add_risktable() +
  add_quantile(y_value = 0.6, color = "gray50", linewidth = 0.75) +
  scale_ggsurvfit()
p +
  # limit plot to show 24 months and less
  coord_cartesian(xlim = c(0, 24)) +
  # update figure labels/titles
  labs(
    y = "Pourcentage sans aménagement",
    title = "Recurrence by Time From Surgery to Randomization",
  )

## Par type d'aménagement 
pAP <- ggsurvfit::survfit2(Surv(time, as.factor(AMENAGEMENT)) ~ 1 , data = suivi_ap) |>
  ggsurvfit(linewidth = 1) +
  add_confidence_interval() +
  add_risktable() +
  add_quantile(y_value = 0.6, color = "gray50", linewidth = 0.75) +
  scale_ggsurvfit()
pAP +
  # limit plot to show 24 months and less
  coord_cartesian(xlim = c(0, 24)) +
  # update figure labels/titles
  labs(
    y = "Pourcentage sans aménagement",
    title = "Recurrence by Time From Surgery to Randomization",
  )
