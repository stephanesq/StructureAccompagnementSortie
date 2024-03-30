# Outputs
# - Liste des éligibles à la LSC à la date de leur première éligibilité
# - Liste des écroués en AP au moment des 2/3 de leur peine et éligibles à la LSC s'ils ne bénéficiaient pas déjà d'une AP

# ORGANISATION
# - Informations sur :
#   - Date écrou initial
# - Date de libération + motif si LC
# 
# - Situ pénale
# - Conserver que 
# - situ pénale avant libération OU si LC avant date de libération prévisionnelle
# - On fait un premier traitement pour déterminer les périodes pendant lesquelles la personne est éligible à la LSC en fonction du QTM, de la catégorie pénale et des 2/3 de la date de libération prev
# - On réunit ces périodes, peu importe la date des 2/3 elle-même


# paramètres ---------
# packages ----
pacman::p_load(tidyverse, arrow, data.table, janitor, haven, styler)
# chemin ------
path = paste0(here::here(),"/Donnees/")
path_dwh = "~/Documents/Recherche/3_Evaluation/_DATA/INFPENIT/"
path_ref = "~/Documents/Recherche/3_Evaluation/_DATA/Referentiel/"

# 1. Import des bases de travail ----
## 1.1. Situ pénale ------

# Traitement :
#   - Renormalisation : une date de situation plutôt que date de début = date de fin  + 1
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

##2.1 Levée écrou et LC
# Pour les LC : Date de levée d'écrou LC +1 
# - Redressement plus simple qu'un travail selon la date de situation pénale
# - et on fait durer les LC jusqu'au lendemain du mouvement (j'imagine que selon les JAP ou les établissements, ils considèrent que la mesure devient effective le lendemain ?)
# - un certain nombre de cas concernés
# - 

h_situ_penale <- t_dwh_h_situ_penale %>% 
  left_join(t_dwh_f_mouvement) 
# %>% 
#   mutate(
#     DT_FIN_SITU_PENALE = case_when(
#       is.na(DT_LEVEECR) ~ DT_FIN_SITU_PENALE,
#       DT_FIN_SITU_PENALE < DT_LEVEECR ~ DT_FIN_SITU_PENALE,
#       LEVEECR_LC==1  ~ DT_LEVEECR+1,
#       TRUE ~  DT_LEVEECR)) %>% 
#   filter(DT_DEBUT_SITU_PENALE <= DT_FIN_SITU_PENALE)  


# test <- t_dwh_h_situ_penale_copie %>% 
#   mutate(
#     situ_post_lib_prev = if_else(DT_SITU_PENALE>DT_LIBE_PREV,1, 0),
#     situ_post_lib = if_else(DT_SITU_PENALE>DT_LEVEECR,1, 0))
# 
# rm(t_dwh_f_mouvement)

##2.2. Ecrou init

h_situ_penale <- h_situ_penale %>% 
  # Date écrou initial
  left_join(t_dwh_ecrou_init)

rm(t_dwh_ecrou_init)