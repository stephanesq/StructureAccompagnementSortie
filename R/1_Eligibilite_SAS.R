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
#   - situ pénale avant libération OU si LC avant date de libération prévisionnelle
#   - Renormalisation : une date de situation plutôt que date de début = date de fin  + 1
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
pacman::p_load(tidyverse, arrow, data.table, janitor, haven, styler,data.table)
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

rm(t_dwh_f_mouvement)

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

# Calcul 2/3 de peine
h_situ_penale[, `:=`(
  DT_DEUXTIERSDEPEINE = floor_date(
    time_length(difftime(DT_LIBE_PREV, DT_ECROU_INITIAL), "days") * 2/3 + DT_ECROU_INITIAL,
    unit = "day"
  ),
  DT_DEUX_ANS_AVT = DT_LIBE_PREV - years(2),
  DT_TROIS_MOIS_AVT = DT_LIBE_PREV - months(3)
)]

# Récupére prochaine situ pénale (shift) ou date lib prev si absente (fcoalesce)
h_situ_penale <- h_situ_penale[order(NM_ECROU_INIT, DT_SITU_PENALE)]
h_situ_penale[, `:=`(
  DT_NEXT_SITU_PENALE = fcoalesce(
    shift(DT_SITU_PENALE, type = "lead"),
    DT_LIBE_PREV)
  ), by = NM_ECROU_INIT]

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
#Supprimer dates de calcul

rm(t_dwh_ecrou_init)

#3. Eligibilité AP/LSC/LSC-D ---------
##3.1. Période d'éligibilité à un aménagement de peine -------
# Créer une colonne pour les groupes consécutifs de ELIG == 1
## cumsum(ELIG==0) crée un groupe cumulatif (1, 1, 2, 2, 2, 2, 1, 2, 2, 2, 2) basé sur les indices où ELIG est égal à 0
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
# 1      2      3      4      5 
# 675016     30      9      2      1 
  