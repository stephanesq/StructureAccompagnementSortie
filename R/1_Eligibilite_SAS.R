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

colselect <- toupper(c("nm_ecrou_init", "dt_debut_situ_penale", "dt_fin_situ_penale", 
                       "cd_cp_globale", "cd_cp_glo_man", "cd_cp_detail", "dt_libe_prev", 
                       "qtm_ferme_tacc_a", "qtm_ferme_tacc_m", "qtm_ferme_tacc_s", "qtm_ferme_tacc_j"))

t_dwh_h_situ_penale <- open_dataset(paste0(path_dwh,"t_dwh_h_situ_penale.parquet"))  |>
  # Définir la requête
    # Filtre et selection
  filter(TP_SOURCE == "GENESIS") |>
    select(colselect) |>
    collect() |>
  janitor::clean_names() |> 
  
  # changement des formats
  mutate(
    across(starts_with("dt_"), as.Date),
    across(starts_with("qtm_"), as.numeric)
    ) |>
    # quantum ferme
    mutate(qtm_ferme_tacc = qtm_ferme_tacc_a * 360 + qtm_ferme_tacc_m * 30 + qtm_ferme_tacc_s * 7 + qtm_ferme_tacc_j) |>
    select(-qtm_ferme_tacc_a, -qtm_ferme_tacc_m, -qtm_ferme_tacc_s, -qtm_ferme_tacc_j)  

## passage en data.table
t_dwh_h_situ_penale = data.table(t_dwh_h_situ_penale)
setkey(t_dwh_h_situ_penale,nm_ecrou_init, dt_debut_situ_penale)
setorder(t_dwh_h_situ_penale,nm_ecrou_init, dt_debut_situ_penale)

#Passe la date de fin en date de début de la ligne suivante  
t_dwh_h_situ_penale[,
                    first_situ := first(dt_debut_situ_penale), #recupere la 1ere observation
                    .(nm_ecrou_init)][,
                                      dt_situ_penale:=fcoalesce(
                                        shift(dt_fin_situ_penale, type = "lead"),
                                        first_situ),
                                      .(nm_ecrou_init)]
# on enlève les dates inutiles
t_dwh_h_situ_penale[, `:=`(
  dt_debut_situ_penale = NULL,
  dt_fin_situ_penale = NULL,
  first_situ = NULL)]

# on garde que ligne exploitable (situ < lib_prev et lib_prev dispo)
t_dwh_h_situ_penale <- t_dwh_h_situ_penale[dt_situ_penale <= dt_libe_prev & !is.na(dt_libe_prev),]

  # catégorie pénale (à partir de la seule situ_penale)
t_dwh_h_situ_penale[, 
                    cd_cp_redr := as.factor(
                      fcase(
                        !cd_cp_globale %in% c('(nd)', '-', ''), as.character(cd_cp_globale),
                        !cd_cp_glo_man %in% c('(ND)', '-', ''), as.character(cd_cp_glo_man),
                        cd_cp_detail %in% c("PR", "PRE", "PRV", "APP", "DAP", "DPO", "OPP", "POU"),  "PR",
                        default = "CO")) ] 
# on enlève les dates inutiles
t_dwh_h_situ_penale[, `:=`(
  cd_cp_globale = NULL,
  cd_cp_glo_man = NULL,
  cd_cp_detail = NULL)]

gc()
## date max 
dt_situ_penale_max <- max(t_dwh_h_situ_penale$dt_situ_penale)


## 1.2. date d'écrou initial  ------
t_dwh_ecrou_init <- open_dataset(paste0(path_dwh,"t_dwh_ecrou_init.parquet")) |>
  collect() |>
  janitor::clean_names() |> 
  select(nm_ecrou_init, dt_ecrou_initial) |>
  mutate(dt_ecrou_initial=as.Date(dt_ecrou_initial)) 

t_dwh_ecrou_init <- t_dwh_ecrou_init %>% 
  distinct() 

## 1.3 Date de libération ----

t_dwh_f_mouvement <- open_dataset(paste0(path_dwh,"t_dwh_f_mouvement.parquet"))

t_dwh_f_mouvement <- t_dwh_f_mouvement|>
  filter(tp_source ==  "GENESIS") |>
  filter(cd_type_mouvement == "LIB" & cd_nature_mouvement == "LEVECR") |>
  select(nm_ecrou_init, dt_mouvement_reel,cd_motif_mouvement) |>
  collect() |>

  group_by(nm_ecrou_init) |>
  mutate(dt_leveecr = max(dt_mouvement_reel)) |>
  ungroup()  |>
  filter(dt_leveecr==dt_mouvement_reel) |> 
  mutate(dt_leveecr=as.Date(dt_leveecr),
         leveecr_lc=if_else(str_detect(cd_motif_mouvement, "^LC") & cd_motif_mouvement != 'LCTRT',1,0)) |>
  select(-dt_mouvement_reel)

gc()

#2. croiser information  ----

##2.1 levée écrou et lc --------------
h_situ_penale <- t_dwh_h_situ_penale %>% 
  left_join(t_dwh_f_mouvement) 
# nettoyage
rm(t_dwh_f_mouvement,t_dwh_h_situ_penale)
##2.2. ecrou init / elibilite lsc --------------
# passage en data.table
# eligibilité en comparant date de mise à l'écrou et date de lib prev
h_situ_penale <- data.table(h_situ_penale)
t_dwh_ecrou_init <- data.table(t_dwh_ecrou_init)

# indexer les tables
# set the on clause as keys of the tables:
setkey(h_situ_penale,nm_ecrou_init)
setkey(t_dwh_ecrou_init,nm_ecrou_init)

# inner join avec date écrou initial nomatch=0 -> autre possibilité merge() de data.table
h_situ_penale <- h_situ_penale[t_dwh_ecrou_init, nomatch = 0]
# nettoyage
rm(t_dwh_ecrou_init)

# calcul 2/3 de peine + date de fin de peine
h_situ_penale[, `:=`(
  dt_deuxtiersdepeine = floor_date(
    time_length(difftime(dt_libe_prev, dt_ecrou_initial), "days") * 2/3 + dt_ecrou_initial,
    unit = "day"
  ),
  dt_deux_ans_avt = dt_libe_prev - years(2),
  dt_trois_mois_avt = dt_libe_prev - months(3),
  dt_fin_peine = if_else(leveecr_lc == 1, dt_libe_prev,dt_leveecr) #date de libération prévisionnelle si sortie pour lc sinon 
)]

# récupére prochaine situ pénale (shift) ou date lib prev si absente (fcoalesce)
h_situ_penale <- h_situ_penale[order(nm_ecrou_init, dt_situ_penale)]
h_situ_penale[, `:=`(
  dt_next_situ_penale = fcoalesce(
    shift(dt_situ_penale, type = "lead"),
    dt_fin_peine)
  ), 
  by = nm_ecrou_init]

# calcul éligibilité
h_situ_penale[, `:=`(
    eligible_ap = fifelse(
      qtm_ferme_tacc > 0 &
        qtm_ferme_tacc/360 <= 5 & 
        dt_deux_ans_avt <= dt_next_situ_penale
    , 1l, 0l),
  eligible_lsc = fifelse(
    qtm_ferme_tacc > 0 &
      qtm_ferme_tacc/360 <= 5 & 
      dt_deux_ans_avt <= dt_next_situ_penale
    , 1l, 0l),
  eligible_lscd = fifelse(
    qtm_ferme_tacc > 0 &
      qtm_ferme_tacc/360 <= 2 & 
      dt_trois_mois_avt <= dt_next_situ_penale ,1l, 0l) #les parties 1l et 0l correspondent à des entiers littéraux en r, pour éviter que r croit à des décimaux ou autres
  )]


#3. eligibilité ap/lsc/lsc-d ---------
##3.1. elig_ap -------
## période d'éligibilité à un aménagement de peine (unique par nm_ecrou_init)
### créer une colonne pour les groupes consécutifs de elig == 1 (séparés par des lignes elig =0)
### cumsum(elig==0) crée un groupe cumulatif (1, 1, 2, 2, 2, 2, 1, 2, 2, 2, 2) basé sur les indices où elig est égal à 0
eligible_ap <- h_situ_penale[, group := cumsum(eligible_ap==0)]
## rleid() crée un identifiant unique pour chaque groupe distinct par id
eligible_ap[, group := rleid(group), by=nm_ecrou_init]
## supprime
eligible_ap <-eligible_ap[eligible_ap==1,]
## dates début et fin
### 
eligible_ap <- eligible_ap[order(nm_ecrou_init, dt_situ_penale, group)]
setkeyv(eligible_ap,c("nm_ecrou_init","group"))
### 
eligible_ap[, `:=`(
  dt_dbt_elig = first(dt_situ_penale),
  dt_fin_elig = last(dt_next_situ_penale))
            , by=.(nm_ecrou_init,group)]
### garde qu'une ligne
eligible_ap <- eligible_ap[, .sd[1], by=.(nm_ecrou_init,group)] # extract first row of groups.
### créé un compteur d'éligibilité
eligible_ap <- eligible_ap[ ,  `:=`( 
  group = seq(.n),
  nb_group = .n,
  annee_dbt_elig_ap = year(dt_dbt_elig))
  , by = .(nm_ecrou_init)]
# table(eligible_ap$group)
# 1      2 
# 660756      1
### qu'écrou avec un seul spell d'éligibilité
eligible_ap <- eligible_ap[nb_group == 1, ]
### enleve les colonnes inutiles
eligible_ap <- eligible_ap[, `:=`(
  group = NULL,
  nb_group = NULL,
  dt_situ_penale = NULL,
  dt_next_situ_penale = NULL,
  eligible_ap = NULL
)]
# nettoyage
rm(h_situ_penale)
## 3.2 elig lsc(-d) ------
## pour lsc, dépend uniquement de l'éligibilité et de la date
## pour lsc-d, manque d'autres infos, pas dispo à ce stade
eligible_ap <- eligible_ap[, `:=`(
  dt_dbt_elig_lsc = fifelse(
    eligible_lsc == 1 & dt_deuxtiersdepeine <= dt_fin_elig, 
    pmax(dt_deuxtiersdepeine, dt_dbt_elig), # pour éviter de prendre une date avant le début de l'éligibilité à l'ap
    NA_date_), #vide sinon
  dt_dbt_elig_lscd = fifelse(
    eligible_lscd == 1 & dt_trois_mois_avt <= dt_fin_elig, 
    pmax(dt_trois_mois_avt, dt_dbt_elig), 
    NA_date_),
  dt_deuxtiersdepeine = NULL,
  dt_trois_mois_avt = NULL,
  eligible_lsc = NULL,
  eligible_lscd = NULL
)]
## 3.3 Filtre et export -----
## Filtres : 
##  - pas de date de fin postérieure à la date de chargement des données
##  - pas d'éligibilité avant 2016 (compris)
eligible_ap <- eligible_ap[dt_fin_elig < dt_situ_penale_max &
                             annee_dbt_elig_ap > 2016
                           , ]
## export
setindexv(eligible_ap,c("nm_ecrou_init")) ### Crée index
setorder(eligible_ap, nm_ecrou_init, dt_dbt_elig) ### Ordonne
write_parquet(eligible_ap,
              paste0(path,"Export/eligible_ap.parquet"),
              compression = "zstd")

# 4. Aménagement de peine ----
### AP dans situ pénit
### On réduit aux NM_ECROU_INIT pertinents
### TOP_ECROUE ==1 : supprime les intervalles avec une libération (TOP_SORTIE_DEF ==0) et plus généralement les pas utiles 

## 4.1 Import -----
## Redressement avec les règles de gestion
t_dwh_h_situ_penit <- open_dataset(paste0(path_dwh,"t_dwh_h_situ_penit.parquet")) |> 
  filter(top_ecroue == 1) |>  
  collect()

t_dwh_h_situ_penit <- t_dwh_h_situ_penit |> 
  select(nm_ecrou_init,
       dt_debut_situ_penit,dt_fin_situ_penit,
       cd_motif_hebergement,top_heberge,cd_statut_semi_liberte, cd_categ_admin,
       top_lsc, cd_type_amenagement, cd_amenagement_peine, 
       dt_debut_exec,dt_suspsl, 
       id_ugc_ref) |>
  # redressement aménagement de peine
  mutate(top_detention = if_else(cd_motif_hebergement %in% c('PE','PSEM','PSE','SEFIP','DDSE'),0,1),    
         AMENAGEMENT = case_when(
           top_detention==0 & cd_motif_hebergement %in% c('PSE', 'PSEM', 'SEFIP') ~ "DDSE", #PSE
           top_detention==0 & cd_motif_hebergement == 'DDSE'  ~ "DDSE",
           top_detention==0 & cd_motif_hebergement == "PE" ~ "PE_nheb",
           top_detention==0 & cd_type_amenagement %in% c('PSE', 'PSEM', 'SEFIP') ~ "DDSE", #PSE
           top_detention==0 & cd_type_amenagement == 'DDSE' ~ "DDSE",
           top_detention==0 & cd_type_amenagement == 'PE' ~ "PE_nheb",
           top_detention==1 & cd_amenagement_peine == "PE" ~ "PE_heb",
           top_detention==1 & cd_amenagement_peine == "SL" & (cd_statut_semi_liberte == "O" | str_detect(cd_categ_admin, "SL")) ~ "SL",
           top_detention==1 & cd_type_amenagement == "SL" & (cd_statut_semi_liberte == "O" | str_detect(cd_categ_admin, "SL")) ~ "SL",
           .default = NA_character_
         )) %>% 
  # Pas possible de filtrer maintenant à cause des problèmes de décalage de date des début de situ penit
  # filter(!is.na(AMENAGEMENT)) |> 
  select(-top_detention, -cd_motif_hebergement, -cd_type_amenagement, -cd_amenagement_peine,-cd_statut_semi_liberte,-cd_categ_admin,-top_heberge) |>
  collect() 
# table(T_DWH_H_SITU_PENIT$TOP_ECROUE,T_DWH_H_SITU_PENIT$TOP_SORTIE_DEF)

## 4.2 Modification ------
## Récupérer les écrous de la table précédente
h_situ_penit <- open_dataset(paste0(path,"Export/eligible_ap.parquet")) |>  
  select(nm_ecrou_init) |> 
  collect() |> 
    inner_join(t_dwh_h_situ_penit) |> 
    distinct() 
## enlève t_dwh
rm(t_dwh_h_situ_penit)
### passage en data.table
h_situ_penit <- data.table(h_situ_penit)
setkey(h_situ_penit,nm_ecrou_init)

### 4.2.2. changement organisation table (data.table) -----
# décalage puis que aménagement et que bonne date
# fill using first : https://rdatatable.gitlab.io/data.table/reference/shift.html
h_situ_penit <- h_situ_penit[order(nm_ecrou_init, dt_debut_situ_penit)]
h_situ_penit[ , dt_situ_penit := 
                as.Date(
                  shift(dt_fin_situ_penit, 
                        fill=dt_debut_situ_penit[1l], 
                        n=1, 
                        type="lag")
                )
            , by=nm_ecrou_init]
# supprime les intervalles
h_situ_penit <- h_situ_penit[,`:=`(
  dt_debut_situ_penit=NULL,
  dt_fin_situ_penit=NULL,
  dt_debut_exec = as.Date(dt_debut_exec),
  dt_suspsl = as.Date(dt_suspsl)
)]
# dédoublonne et tri
h_situ_penit <- unique(h_situ_penit)
h_situ_penit <- h_situ_penit[order(nm_ecrou_init, dt_situ_penit)]
# identifier les périodes avec aménagement
# utiliser les dt_debut_exec et les dt_suspsl pour modifier ces périodes
h_situ_penit <- h_situ_penit[,`:=`(
  dt_situ_penit = fifelse(
    !is.na(dt_debut_exec) & !is.na(amenagement) & #date et aménagement renseigné
      dt_debut_exec>dt_situ_penit & 
      ( dt_debut_exec <= shift(dt_situ_penit, n=1, type="lead") | 
        is.na(shift(dt_situ_penit, n=1, type="lead"))
      ) #si la date de début d'exécution commence avant la ligne suivante ou qu'il n'y en a pas
    ,dt_debut_exec
    ,dt_situ_penit
  ),
  amenagement = fifelse(
    !is.na(dt_debut_exec) & !is.na(amenagement) & #date et aménagement renseigné
      dt_debut_exec>dt_situ_penit & 
      ( dt_debut_exec <= shift(dt_situ_penit, n=1, type="lead") | 
          is.na(shift(dt_situ_penit, n=1, type="lead"))
      ) #si la date de début d'exécution commence avant la ligne suivante ou qu'il n'y en a pas
    , NA_character_
    , amenagement
    ))]
# pareil avec suspension 
h_situ_penit <- h_situ_penit[,`:=`(
  Dt_situ_penit = fifelse(
    !is.na(dt_suspsl) & !is.na(amenagement) & year(dt_suspsl)>1900 & #date et aménagement renseigné
      dt_suspsl>=dt_situ_penit & (
        dt_suspsl <= shift(dt_situ_penit, n=1, type="lead") | 
        is.na(shift(dt_situ_penit, n=1, type="lead"))
              )
    ,dt_suspsl
    ,dt_situ_penit
  ),
  amenagement = fifelse(
      !is.na(dt_suspsl) & !is.na(amenagement) & year(dt_suspsl)>1900 & #date et aménagement renseigné
        dt_suspsl>=dt_situ_penit & (
          dt_suspsl <= shift(dt_situ_penit, n=1, type="lead") | 
            is.na(shift(dt_situ_penit, n=1, type="lead"))
        )
      ,NA_character_
      ,amenagement
    ),
  indic_amenagement = fifelse(is.na(amenagement),0,1)
  )]
# supprime les intervalles
h_situ_penit <- h_situ_penit[,`:=`(
  dt_debut_exec=NULL,
  dt_suspsl=NULL
)]
## 4.3 Phase d'AP ------
### 4.3.1 Date suivante ou levée d'écrou ------
# libération -> data.table
f_levecr <- open_dataset(paste0(path_dwh,"t_dwh_f_mouvement.parquet")) |>
  filter(tp_source ==  "GENESIS") |>
  filter(cd_type_mouvement == "LIB" & cd_nature_mouvement == "LEVECR") |>
  select(nm_ecrou_init, dt_mouvement_reel) |>
    group_by(nm_ecrou_init) |>
    summarise(dt_leveecr = max(dt_mouvement_reel)) |>
  collect() 
### passage en data.table
f_levecr <- data.table(f_levecr)
setkey(f_levecr,nm_ecrou_init)  
### récupérer l'info puis supprile
h_situ_penit <- merge(h_situ_penit,f_levecr)
rm(f_levecr)
# date suivante/levée d'écrou pour la penit -> afin d'avoir une date de fin si besoin
h_situ_penit[, `:=`(
  dt_next_situ_penit = fcoalesce(
    shift(dt_situ_penit, type = "lead"),
    as.Date(dt_leveecr))
), by = nm_ecrou_init]
gc()
### 4.2.3. phase d'ap ------
## période d'éligibilité à un aménagement de peine (unique par nm_ecrou_init)
### créer une colonne pour les groupes consécutifs de elig == 1 (séparés par des lignes elig =0)
### cumsum(elig==0) crée un groupe cumulatif (1, 1, 2, 2, 2, 2, 1, 2, 2, 2, 2) basé sur les indices où elig est égal à 0
situ_penit_ap <- h_situ_penit[, group := cumsum(indic_amenagement==0)]
rm(h_situ_penit)
## rleid() crée un identifiant unique pour chaque groupe distinct par id
situ_penit_ap[, group := rleid(group), by=nm_ecrou_init]
## supprime
situ_penit_ap <-situ_penit_ap[indic_amenagement==1,]
## dates début et fin
### 
situ_penit_ap <- situ_penit_ap[order(nm_ecrou_init, dt_situ_penit, group)]
setkeyv(situ_penit_ap,c("nm_ecrou_init","group"))
### 
situ_penit_ap[, `:=`(
  dt_dbt_ap = first(dt_situ_penit),
  dt_fin_ap= last(dt_next_situ_penit))
  , by=.(nm_ecrou_init,group)]
### garde qu'une ligne
situ_penit_ap <- situ_penit_ap[, .sd[1], by=.(nm_ecrou_init,group)] # extract first row of groups.
### créé un compteur d'éligibilité
situ_penit_ap <- situ_penit_ap[ ,  `:=`( 
  group = seq(.N),
  nb_group = .N,
  annee_dbt_ap = year(dt_dbt_ap))
  , by = .(nm_ecrou_init)]
# table(situ_penit_ap$group)
# 1      2      3      4      5      6      7      8      9     10 
# 241888  11521   1257    188     37     13      6      4      2      1 
### qconserve que la première phase d'ap -> pour l'instant pas besoin 
# situ_penit_ap <- situ_penit_ap[group == 1, ]
### enleve les colonnes inutiles
dt_situ_penit_max <- max(situ_penit_ap$dt_situ_penit)
situ_penit_ap <- situ_penit_ap[, `:=`(
  dt_situ_penit = NULL,
  dt_next_situ_penit = NULL,
  indic_amenagement = NULL
)]
## 4.4 filtre et export -----
## filtres : 
##  - pas de date de fin postérieure à la date de chargement des données
##  - pas d'éligibilité avant 2016 (compris)
situ_penit_ap <- situ_penit_ap[dt_dbt_ap <= dt_situ_penit_max &
                               annee_dbt_ap > 2016
                           , ]
## export
setindexv(situ_penit_ap,c("nm_ecrou_init")) ### crée index
setorder(situ_penit_ap, nm_ecrou_init, dt_dbt_ap) ### Ordonne
write_parquet(situ_penit_ap,
              paste0(path,"Export/situ_penit_ap.parquet"),
              compression = "zstd")


# 5. Table finale ----
## 5.1. Import -----
suivi_ap <-  open_dataset(paste0(path,"Export/eligible_ap.parquet")) |>
  select(-annee_dbt_elig_ap, -dt_deux_ans_avt) |> 
  collect() |> 
  left_join(
    open_dataset(paste0(path,"Export/situ_penit_ap.parquet")) |> 
      select(-dt_leveecr, -annee_dbt_ap) |> 
      collect(),
    by = join_by(nm_ecrou_init,
                 closest(dt_dbt_elig <= dt_dbt_ap))
  ) 
suivi_ap <- data.table(suivi_ap)
### Filtre dates incohérentes (> à observation de l'autre)
dt_situ_penal_max <- max(suivi_ap$dt_fin_elig, na.rm = t) #max de la situation pénale
dt_situ_penit_max <- max(suivi_ap$dt_dbt_ap, na.rm = t) #max de la situ penit
dt_max <- pmin(dt_situ_penit_max,dt_situ_penale_max) # la plus petite des deux
suivi_ap <- suivi_ap[dt_fin_elig <= dt_max & 
                       (is.na(dt_dbt_ap) | dt_dbt_ap <= dt_max), ] #on retire les infos post

## 5.2 lc ------
### 5.2.1 avec les levées d'écrou (modif aménagement et date) ------
suivi_ap <- suivi_ap[, `:=`(
  amenagement = fcase(
    is.na(amenagement) & leveecr_lc == 1, "LC", 
    !is.na(amenagement), amenagement,
    default = "Non_AP")
)]
## modif date
suivi_ap <- suivi_ap[amenagement == "LC", `:=`(
  dt_dbt_ap = dt_leveecr,
  dt_fin_ap = dt_fin_peine
  )]
## crée une variable générale
suivi_ap <- suivi_ap[, `:=`(
  ap = fifelse(amenagement == "Non_AP",0,1)
)]

### 5.2.2. dans les mesures -----
## 30150 30160 30520 (REVOCATION TOTALE LC)

## 5.3 Export -----
### 5.3.1 Aménagement a la mise sous écrou + filtre ----
suivi_ap <- suivi_ap[, `:=`(
  amenagement_mse = fifelse(dt_dbt_ap <=  dt_ecrou_initial + days(7) & !is.na(dt_dbt_ap), 1, 0),
  dt_libe_prev = NULL,
  cd_motif_mouvement = NULL,
  dt_leveecr = NULL,
  leveecr_lc = NULL 
)]
### 5.3.2. parquet
setindexv(suivi_ap,c("nm_ecrou_init")) ### crée index
setorder(suivi_ap, nm_ecrou_init, dt_dbt_elig, dt_dbt_ap) ### Ordonne
write_parquet(suivi_ap,
              paste0(path,"Export/suivi_ap.parquet"),
              compression = "zstd")

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
  ggcuminc(linewidth = 1) +
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
