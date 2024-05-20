# packages ----
pacman::p_load(tidyverse, arrow, data.table, janitor, haven, ggsurvfit)

# paramètres ----

# chemin ------
path = paste0(here::here(),"/Donnees/")
path_data = "~/Documents/Recherche/3_Evaluation/_DATA/"
path_dwh = "~/Documents/Recherche/3_Evaluation/_DATA/INFPENIT/"
path_ref = "~/Documents/Recherche/3_Evaluation/_DATA/Referentiel/"
path_ref_ip = "~/Documents/Recherche/3_Evaluation/_DATA/Referentiel/"

#sur site
# path = "L:/SDEX/EX3/_EVALUATION_POLITIQUES_PENITENTIAIRES/COMMANDES/2023-06 - SAS - IP1/Donnees/"
# path_dwh = "L:/SDEX/EX3/_ANALYSES_ET_ETUDES/TABLES_DE_DONNEES/R_import/"
# path_ref = "L:/SDEX/EX3/_ANALYSES_ET_ETUDES/TABLES_DE_DONNEES/tables_infpenit/"
# path_ref_ip = "L:/SDEX/EX3/_ANALYSES_ET_ETUDES/TABLES_DE_DONNEES/tables_IP/"

# 1. Import ------
## 1.1. Eligibles -----
suivi_ap <-  open_dataset(paste0(path,"Export/suivi_ap.parquet")) |> 
  collect() |> 
  janitor::clean_names()

suivi_ap <- data.table(suivi_ap)
suivi_ap <- suivi_ap[amenagement_mse == 0,] 
# index
setkey(suivi_ap,nm_ecrou_init)

## 1.2. UGC ds situ_penit ----
### 1.2.1. traitement import situ_penit -----
situ_cols <- c('nm_ecrou_init',
               'dt_debut_situ_penit', 'dt_fin_situ_penit', 
               'cd_etablissement','id_ugc_ref', 'cd_categ_admin', 
               'top_ecroue')

if(!exists("situ_penit")){}
situ_penit  <- read_parquet(str_glue("{path_dwh}t_dwh_h_situ_penit.parquet"),
                              col_select = toupper(situ_cols)) %>% 
  clean_names() |>  
  filter(top_ecroue == 1) |>  
  mutate(across(where(is.character), ~ if_else( . %in% c("NA","(ND)","(NF)","(NR)"), NA, .) )) |> 
  mutate(across(starts_with("dt_"), as.Date)) |> 
  select(-top_ecroue) |> 
  as.data.table()

# index
setkey(situ_penit,nm_ecrou_init)

#jointure eligibles
eligible_ugc_penit <- situ_penit[suivi_ap, 
                            on = .(nm_ecrou_init), 
                            nomatch = 0][ #innerjoin
  order(nm_ecrou_init, dt_debut_situ_penit)]

#suppri situ_penit
rm(situ_penit)

### 1.2.2. RED1 : date situ -----
# modif var de date : première ou fin de la précédente
setorder(eligible_ugc_penit,nm_ecrou_init,dt_debut_situ_penit)

eligible_ugc_penit[, `:=`(
  date_situ_ugc = fcoalesce(
    shift(dt_fin_situ_penit, type = "lead"),
    dt_debut_situ_penit)
), 
by = nm_ecrou_init]  

# nettoyage
eligible_ugc_penit <- eligible_ugc_penit[id_ugc_ref > -3,] #id_ugc renseigné
eligible_ugc_penit <- eligible_ugc_penit[,
                           `:=`(
                             dt_debut_situ_penit = NULL,
                             dt_fin_situ_penit = NULL
                           )]

### 1.2.3. Export ----

setindexv(eligible_ugc_penit,c("nm_ecrou_init","id_ugc_ref")) ### Crée index
setorder(eligible_ugc_penit, nm_ecrou_init, date_situ_ugc) ### Ordonne

write_parquet(eligible_ugc_penit, 
              paste0(path,"Export/eligible_ugc_penit.parquet"),
              compression = "zstd")

## 1.3. Info cellule ----
## 
### 1.3.1. Jointure avec cellule_red_etab ----

#### Cellule etab
cellule_etab <- read_parquet(paste0(path,"Export/cellule_etab.parquet")) |> 
  select(-dt_fermeture, -n, -fl_indisp, -lc_code) |> 
  rename("cellule_date_situ_ugc"="date_situ_ugc")

cellule_etab = data.table(cellule_etab)
setkey(cellule_etab,id_ugc)

#### eligible_ugc_penit
eligible_ugc_penit <- read_parquet(paste0(path,"Export/eligible_ugc_penit.parquet"))

eligible_ugc_penit = data.table(eligible_ugc_penit)
setkey(eligible_ugc_penit,nm_ecrou_init,id_ugc_ref)

#jointure avec data.table
eligible_ugc_penit[cellule_etab,
                  on=.(id_ugc_ref = id_ugc,
                       cd_etablissement = cd_etablissement,
                       date_situ_ugc>=cellule_date_situ_ugc)] # même id_ugc, info précédente dans table cellule

eligible_quartier <- eligible_quartier |> 
  select(-cellule_date_situ_ugc) 

#supprime
rm(cellule_etab,eligible_ugc_penit)

### 1.3.2. FILL + Synthese selon quartier ----

### FILL cd_categ_admin_quartier
eligible_quartier <- eligible_quartier |> 
  group_by(nm_ecrou_init, id_ugc_ref) |> 
  arrange(date_situ_ugc) |> 
  fill(cd_categ_admin_quartier, .direction = c("updown")) |> 
  ungroup() 

### variable synthese quartier
eligible_quartier <- eligible_quartier[,
                                       cd_quartier_simple := 
                                         fcase(str_detect(cd_categ_admin_quartier,"SAS"), "SAS",
                                               str_detect(cd_categ_admin_quartier,"QPA"), "CPA/QPA",
                                               str_detect(cd_categ_admin_quartier,"MC"), "MC/QMC",
                                               str_detect(cd_categ_admin_quartier,"CD"), "CD/QCD",
                                               str_detect(cd_categ_admin_quartier,"MA"), "MA/QMA",
                                               str_detect(cd_categ_admin_quartier,"SL"), "CSL/QSL",
                                               str_detect(cd_categ_admin_quartier,"EPM"), "EPM",
                                               str_detect(cd_categ_admin_quartier,"AUT"), "AUT")]
### Rajout info TJ 
ref_etab_tgi <- read.csv2(paste0(path_ref,"ep_tgi.csv"),
                          encoding = "ISO-8859-1")

### réduction ligne
eligible_quartier_ap <- eligible_quartier[,
                               .(date_situ_ugc = min(date_situ_ugc.x)),
                               .(nm_ecrou_init,cd_etablissement,
                                 qtm_ferme_tacc,dt_ecrou_initial, dt_fin_peine, 
                                 dt_dbt_elig, dt_fin_elig, dt_dbt_elig_lsc, dt_dbt_elig_lscd, 
                                 top_lsc, amenagement, dt_dbt_ap, dt_fin_ap, ap, 
                                 cd_quartier_simple
                                ) #cd_categ_admin_red,cd_categ_admin_quartier,cd_categ_admin_place, cd_categ_admin_detenu,capa_theo,
                      ]

# eligible pas aménagés
eligible_quartier_ap <- eligible_quartier_ap[is.na(dt_dbt_ap) | dt_dbt_ap > date_situ_ugc,]
# cd_categ_admin renseigné
eligible_quartier_ap <- eligible_quartier_ap[!is.na(cd_categ_admin_red),]

# temps entre Changement
setorder(eligible_quartier_ap,nm_ecrou_init,date_situ_ugc)
eligible_quartier2[, `:=`(
  date_next_situ_ugc = fcoalesce(
    shift(date_situ_ugc, type = "lag"),
    dt_fin_elig)
), 
by = nm_ecrou_init]  

### 1.3.3. Export ----
setindexv(eligible_quartier_ap,c("nm_ecrou_init")) ### Crée index
setorder(eligible_quartier_ap, nm_ecrou_init, date_situ_ugc) ### Ordonne

write_parquet(eligible_quartier_ap, paste0(path,"Export/eligible_quartier_ap.parquet"))


### 5.1.2. Variables nécessaires ------
## Durée observation
eligible_quartier2[, duree_observation := time_length(interval(date_situ_ugc, date_next_situ_ugc), unit = "months")]
## Durée AP + imputer pour éviter égalité avec une décimale
eligible_quartier2[, duree_ap := time_length(interval(date_situ_ugc, dt_dbt_ap), unit = "months")]
eligible_quartier2[, duree_ap_impute := duree_ap + runif(.N)]
## Temps
eligible_quartier2[, time := duree_observation]
eligible_quartier2[ap == 1, time := duree_ap_impute]
## SAS
eligible_quartier2[, SAS := "hors SAS"]
eligible_quartier2[str_detect(cd_categ_admin_red,"^SAS"), SAS := "SAS"]


## 5.2 Kapman Meier -----
# https://www.danieldsjoberg.com/ggsurvfit/
# KM
p <- survfit2(Surv(time, ap) ~ SAS, data = eligible_quartier2) |>
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
    title = "Temps passé depuis un changement de quartier",
  )

  # contrôle sur les premières apparitions des id_ugc...
situ_penit[id_ugc_ref > -3, .(min_dt = min(dt_debut_situ_penit), max_dt = max(dt_fin_situ_penit))]
situ_penit[id_ugc_ref_histo > -3, .(min_dt = min(dt_debut_situ_penit), max_dt = max(dt_fin_situ_penit))]
# tout s'explique (presque). la variable id_ugc n'existe que depuis janvier 2023, d'où l'impossibilité de trouver des observations avant 2023 en fonction de la cellule. CD_CATEG_ADMIN C PARTI 



# on garde toutes les observations où qqn est dans une UGC SAS
## par l'id_ugc (valable à partir de janvier 2023)
ecrou_sas_ugc <- situ_penit[l_sas, on = .(id_ugc_ref = id_ugc, cd_etablissement), nomatch = NULL][order(nm_ecrou_init, dt_debut_situ_penit)]

## par le cd_categ_admin
ecrou_sas_cca <- situ_penit[
  # cd_categ_admin qui commence par SAS 
  cd_categ_admin %like% "^SAS" |
    # ou établissements dont la SAS est ouverte depuis 2022, et qui était toujours inscrite comme QPA dans topo 
    # Poitiers, Bordeaux, Longuenesse, Aix
    (cd_etablissement %chin% c("00637072","00641743","00912148","00641740") & cd_categ_admin %like% "^QCPA")
][order(nm_ecrou_init, dt_debut_situ_penit)
][, sas_sl := fifelse(cd_categ_admin %like% "^SASSL|QCPASL" | id_ugc_ref %in% l_sas[sas_sl == T, id_ugc], T, F)]

# /!\ SAS AIX & POITIERS ont des SASSL alors que officiellement, non. correction
ecrou_sas_cca[cd_etablissement %chin% c("00641740", "00637072"), sas_sl := F]

# on garde les observations qui ont lieu après la date min des SAS dans le SRJ 
ecrou_sas_cca <- ecrou_sas_cca %>% 
  filter(dt_debut_situ_penit > min(srj_sas$d_app)) %>% 
  left_join(ref_etab) %>% 
  left_join(srj_sas, by = join_by(cd_etablissement == cd_etab_rat))

## on combine les deux, funion() s'occupe de ne garder que les lignes distinctes (puisque risques de doublons)
ecrou_sas_raw <- funion(ecrou_sas_ugc, ecrou_sas_cca)[order(nm_ecrou_init, dt_debut_situ_penit)]

ecrou_sas_raw <- ecrou_sas_raw[!is.na(lc_sas)]

# date min (max) dt_debut_situ_penit (dt_fin_situ_penit) = date entrée (sortie) SAS
ecrou_sas_raw[, min_dt := min(dt_debut_situ_penit), nm_ecrou_init]
ecrou_sas_raw[, max_dt := max(dt_fin_situ_penit), nm_ecrou_init]

# max(dt_debut_exec) ne fonctionne pas dans ecrou_sas, meme avec na.rm = T. j'essaie de la copier à toutes les lignes
ecrou_sas_raw[, test_dt_mex := fifelse(is.na(dt_debut_exec), ymd("1900-01-01"), ymd(dt_debut_exec))]

# compteurs de lignes, par nm_ecrou_init 
ecrou_sas_raw[order(nm_ecrou_init, dt_debut_situ_penit), rang_sas := rowid(nm_ecrou_init, lc_sas)]
ecrou_sas_raw[order(nm_ecrou_init, -dt_debut_situ_penit), rang_sas_inv := rowid(nm_ecrou_init, lc_sas)]


# fonction pour compter le nb de nm_ecrou_init distincts par SAS et par mois 
effectif_mens_long <- function(dt, var_date){
  
  tab_long <- dt[, uniqueN(nm_ecrou_init), 
                 keyby = .(lc_sas, year(get(var_date)), lubridate::month(get(var_date), label = T))]
  
  tab_long
  tab_large <- dcast(tab_long, lc_sas + year ~ lubridate, fill = 0, value.var = "V1")
  Total <- tab_large[, rowSums(.SD), .SDcols=-(1:2)]
  cbind(tab_large, Total)
}

# nb d'écroués par mois, dans chaque SAS (flux complet, suivi)
effectif_mens_long(ecrou_sas_raw, "dt_debut_situ_penit")
effectif_mens_long(ecrou_sas_raw[sas_sl==F], "dt_debut_situ_penit") # hors places SL 

# pour comparaison avec sorties finales
effectif_mens_long(ecrou_sas_raw, "min_dt")
effectif_mens_long(ecrou_sas_raw[year(max_dt) < 9000], "max_dt")

# aménagements ----

# on crée top_detention
ecrou_sas_raw[, top_detention := fifelse(cd_motif_hebergement %chin% c('PE','PSEM','PSE','SEFIP','DDSE'), F, T)]
ecrou_sas_raw[,.N,.(top_detention, top_heberge)]
ecrou_sas_raw[top_detention==F] 

# on redresse l'aménagement de cd_amenagement_peine, en prenant en compte les dates de début d'exécution et de suspension de SL
# conséquences :
# - si un AP a été accordé mais que la date de MEX est supérieur à la date d'extraction de situ_penit, comptera comme pas d'AP POUR L'INSTANT
# - si un AP-SL a été accordé dans le passé de l'écroué, puis suspendu, alors comptera comme pas d'AP 
ecrou_sas_raw[cd_amenagement_peine %chin% c('SL', 'PE', 'PSE', 'SEFIP', 'PSEM', 'DDSE', 'DDSEAM')
              & dt_debut_exec <= dt_max # on vérifie que la mesure est en cours d'exécution à date donnée
              & (dt_suspsl > dt_max | is.na(dt_suspsl) | year(dt_suspsl) == 1990), # mesure non suspendue
              cd_amenagement_peine_2 := cd_amenagement_peine
]

ecrou_sas_raw[, tabyl(.SD, cd_amenagement_peine, cd_amenagement_peine_2)]

# aménagement de peine non-hébergé 
# avec priorisation sur cd_motif_hebergement [relire fiche sur les règles de gestion]
ecrou_sas_raw[top_detention == F, 
              amenagement := fcase(cd_motif_hebergement %in% c('PSE', 'PSEM', 'SEFIP', 'DDSE'), 'DDSE',
                                   cd_motif_hebergement == "PE", "PE_NHEB",
                                   
                                   cd_type_amenagement %in% c('PSE', 'PSEM', 'SEFIP', 'DDSE'), 'DDSE',
                                   cd_type_amenagement == "PE", "PE_NHEB",
                                   
                                   cd_amenagement_peine_2 %in% c('PSE', 'PSEM', 'SEFIP', 'DDSE', 'DDSEAM'), 'DDSE',
                                   cd_amenagement_peine_2 == "PE", "PE_NHEB",
                                   default = "CHELOU"
              )]

# aménagement de peine hébergé 
# nb : DDSEAM en cd_amenagement_peine_2 => dt MEX correspond à fin de la présence en SAS, et d'après quelques fiches pénales,
# ce sont des écroués qui sont ensuite transférés dans un autre établissement. 
ecrou_sas_raw[top_detention == T,
              amenagement := fcase(cd_amenagement_peine_2 == "PE", "PE_HEB",
                                   cd_amenagement_peine_2 == "SL", "SL",
                                   default = NA)]

ecrou_sas_raw[, .N, keyby = .(top_detention, amenagement)]

# on remet à 0 top_lsc des personnes qui ne sont pas en aménagement
ecrou_sas_raw[is.na(amenagement), top_lsc := 0]

## mise à exécution de l'AP ----

### à l'entrée de la SAS ? ----

# l'AP est déjà saisi à l'entrée dans la SAS, même si en attente de mise à exécution
ecrou_sas_raw[, ap_entree := F]
ecrou_sas_raw[rang_sas == 1 & !is.na(amenagement), ap_entree := T]
# liste des id d'écroués avec entrée aménagée en SAS
l_entree_ap <- ecrou_sas_raw[ap_entree == T, unique(nm_ecrou_init)]
# on copie ap_entree sur toutes les lignes de l'écroué
ecrou_sas_raw[nm_ecrou_init %chin% l_entree_ap, ap_entree := T]

### à la sortie de la SAS ? ----
# certains AP peuvent commencer plusieurs jours après la sortie de SAS (eg 6370720010078, qui en plus n'a pas la LSC directement inscrite...)
ecrou_sas_raw[, ap_sortie := fifelse(as.Date(max_dt) <= as.Date(dt_debut_exec), T, F, na = F)]

### au milieu (wrong word krkrkr) de la SAS ? ----
ecrou_sas_raw[, ap_pendant := F]
# si aucun aménagement à l'entrée et à la sortie + début d'exécution de l'AP au cours du passage en SAS => ap_pendant = T
ecrou_sas_raw[ap_entree == F & ap_sortie == F 
              & !is.na(cd_amenagement_peine_2) 
              & dt_debut_exec %between% .(min_dt, max_dt), ap_pendant := T]

# entrée sur une place SL ----
ecrou_sas_raw[, entree_place_sl := F]
ecrou_sas_raw[rang_sas == 1 & sas_sl == T, entree_place_sl := T]

# entrée avec aménagement SL ---
ecrou_sas_raw[, entree_ap_sl := F]
ecrou_sas_raw[rang_sas == 1 & amenagement == "SL", entree_ap_sl := T]

# réduction de la table : une ligne par écroué ----
ecrou_sas <- ecrou_sas_raw[, .(dt_deb_sas = min(dt_debut_situ_penit) - days(1),
                               dt_fin_sas = max(dt_fin_situ_penit),
                               dt_mex_ap = max(test_dt_mex),
                               top_lsc = max(top_lsc),
                               entree_place_sl = max(entree_place_sl),
                               entree_ap_sl = max(entree_ap_sl),
                               ap_in = max(ap_entree),
                               ap_while = max(ap_pendant),
                               ap_out = max(ap_sortie))
                           , keyby = .(nm_ecrou_init, cd_etablissement, lc_sas)]

ecrou_sas[, .N, keyby = .(ap_in, ap_while, ap_out)]

## ajout des situations pénitentiaires ayant lieu le jour **après** la sortie de SAS ----
# liste de ceux qui ne sont plus en SAS
a <- ecrou_sas[year(dt_fin_sas) < 9000, dt_fin_sas, .(nm_ecrou_init, cd_etablissement)]
# on joint à situ_penit, en ne gardant que la première ligne après la dernière date de recensement SAS
sp_sortants <- situ_penit[a, on = .(nm_ecrou_init)
][dt_debut_situ_penit > dt_fin_sas
][order(dt_debut_situ_penit), head(.SD, 1), .(nm_ecrou_init)]

sp_sortants[,.N, .(top_ecroue, top_sortie_def, top_evade)]

# on refait les mêmes op aménagements que dans ecrou_sas_raw
# top_detention
sp_sortants[, top_detention := T]
sp_sortants[#top_sortie_def == "1" |
  cd_motif_hebergement %chin% c('PE','PSEM','PSE','SEFIP','DDSE'),
  top_detention := F]

# on redresse l'aménagement de cd_amenagement_peine
sp_sortants[cd_amenagement_peine %chin% c('SL', 'PE', 'PSE', 'SEFIP', 'PSEM', 'DDSE', 'DDSEAM')
            & dt_debut_exec <= dt_max # on vérifie que la mesure est en cours d'exécution à date donnée
            & (dt_suspsl > dt_max | is.na(dt_suspsl) | year(dt_suspsl) == 1990), # mesure SL non suspendue 
            # & top_sortie_def == "0", # toujours écroué 
            cd_amenagement_peine_2 := cd_amenagement_peine]

sp_sortants[, tabyl(.SD, cd_amenagement_peine, cd_amenagement_peine_2)]

# aménagement de peine non-hébergé 
sp_sortants[top_detention == F, 
            amenagement := fcase(cd_motif_hebergement %in% c('PSE', 'PSEM', 'SEFIP', 'DDSE'), 'DDSE',
                                 cd_motif_hebergement == "PE", "PE_NHEB",
                                 
                                 cd_type_amenagement %in% c('PSE', 'PSEM', 'SEFIP', 'DDSE'), 'DDSE',
                                 cd_type_amenagement == "PE", "PE_NHEB",
                                 
                                 cd_amenagement_peine_2 %in% c('PSE', 'PSEM', 'SEFIP', 'DDSE', 'DDSEAM'), 'DDSE',
                                 cd_amenagement_peine_2 == "PE", "PE_NHEB",
                                 default = "CHELOU"
            )]

# aménagement de peine hébergé 
sp_sortants[top_detention == T,
            amenagement := fcase(cd_amenagement_peine_2 == "PE", "PE_HEB",
                                 cd_amenagement_peine_2 == "SL", "SL",
                                 default = NA)]

sp_sortants[,.N,keyby = .(top_ecroue, top_sortie_def, top_evade, amenagement)]

# aménagement de peine en attente d'exécution (apparait comme is.na(amenagement), eg 6417400086916)
sp_sortants[is.na(amenagement) & !is.na(cd_amenagement_peine_2) & dt_debut_situ_penit < dt_debut_exec]
# très peu de cas. je laisse tomber.

# on remet à 0 top_lsc des personnes qui ne sont pas en aménagement
sp_sortants[is.na(amenagement), top_lsc := 0]

# ~sorties de détention~ /!\ J'EXCLUS LES EVADES !!!!
sp_sortants[, sortie_def_sas := F]
sp_sortants[top_sortie_def == "1" | !is.na(amenagement) & top_evade == "0", sortie_def_sas := T]

# aménagement à la sortie 
sp_sortants[, ap_sortie_sas := F]
sp_sortants[!is.na(amenagement) & sortie_def_sas == T, ap_sortie_sas := T]

sp_sortants[,.N, keyby=.(sortie_def_sas, ap_sortie_sas)]

# réduc
sortie_sas <- sp_sortants[, .(nm_ecrou_init, top_lsc_s = top_lsc, sortie_def_sas, ap_sortie_sas)]


## LSC et LSCPD ----

# Données du baromètre LSC de plein droit 
# parmi la liste des fichiers dispo, on prend le plus récent 
if(!exists("lscpd_raw")){
  path_data_lscpd <- "K:/Baromètre_LSCPD/Données_finales/"
  file_lscpd <- last(list.files(path_data_lscpd, pattern = "^LSCPD_Détenus_"))
  lscpd_raw <- readxl::read_excel(paste0(path_data_lscpd, file_lscpd)) %>% clean_names()
}

lscpd <- lscpd_raw %>% 
  mutate(across(starts_with("date_"), as.Date),
         prononce_mois = lubridate::month(prononce_date, label = T),
         prononce_lgl = if_else(prononce == "Accordé", T, F),
         # on recode les aménagements pour que ça corresponde à ceux de la table sortie 
         amenagement = fcase(str_starts(regime, "DDSE"), "DDSE",
                             str_starts(regime, "Libération"), "LC",
                             str_starts(regime, "Placement") & regime_detail != "Hébergé", "PE_NHEB",
                             str_starts(regime, "Placement"), "PE_HEB",
                             str_starts(regime, "Semi"), "SL",
                             default = "NA"))

# on garde les LSCPD accordées 
lscpd_granted <- lscpd %>% 
  filter(prononce_lgl == T) %>% 
  select(nm_ecrou_init, cd_etablissement = prononce_code_etab, prononce_date, date_debut, date_fin, amenagement) %>% 
  mutate(top_lscpd = 1) %>% 
  as.data.table()

lscpd_granted[ecrou_sas, on = .(nm_ecrou_init)][top_lscpd == 1,.N,keyby=.(lc_sas)]

# mouvements pour LSC classiques : top_lsc de situ_penit pas exhaustif (cf règle de gestion LSC) + top_lsc_mesure & fl_lsc_mesure trop récents pour que ce soit applicable à tous les écroués SAS.
# obligée de prendre les mouvements d'AP associés à une décision de LSC 
mouv_cols <- c("nm_ecrou_init", "dt_mouvement_reel", "cd_type_mouvement", "cd_nature_mouvement",
               "cd_motif_mouvement", "top_lsc")

if(!exists("mouv_temp")){
  mouv_temp <- read_parquet(str_glue("{path_dwh}t_dwh_f_mouvement.parquet"),
                            col_select = toupper(mouv_cols)) %>% 
    clean_names() %>% rename(top_lsc_mvt = top_lsc) %>% 
    as.data.table()
}

# on prend les mouvements de LSC pour tous les écroués de la SAS 
mouv_sortants <- mouv_temp[ecrou_sas, 
                           on = .(nm_ecrou_init), nomatch = NULL][top_lsc_mvt == "1"]

# on réduit à une ligne par écroué, ayant au moins un mouvement de LSC compris entre le début et la fin de la présence en SAS,
# ou dont la date de mouvement est égale à la date de mise à exécution de la mesure d'aménagement de peine renseignée
# dans situ_penit. à peu près certaine que ça correspondra bien à un "effet" de la SAS
lsc_mvt <- mouv_sortants[, lapply(.SD, as.Date), 
                         .SDcols = names(mouv_sortants) %like% "^dt",
                         .(nm_ecrou_init)
][dt_mouvement_reel %between% .(dt_deb_sas, dt_fin_sas) | dt_mouvement_reel == dt_mex_ap,
  unique(nm_ecrou_init)]

## jointures [] FAIRE PAR CODE ETAB LA PROCHAINE FOIS!!! ----
ecrou_sas
sortie_sas
lscpd_granted

t1 <- sortie_sas[ecrou_sas, on = .(nm_ecrou_init)]
t2 <- lscpd_granted[t1, on = .(nm_ecrou_init)]

# derniers redressements LSC/LSCPD, avec priorité sur LSCPD
t2[, top_lscpd_red := F]
t2[top_lscpd == 1 & sortie_def_sas == T, top_lscpd_red := T]

t2[, top_lsc_red := F]
t2[top_lscpd_red == F & (top_lsc == 1 | top_lsc_s == 1 | nm_ecrou_init %chin% lsc_mvt), top_lsc_red := T]

t2[,.N,keyby=.(top_lscpd_red, top_lsc_red, ap_sortie_sas)]

# type sortie détaillée, avec priorité LSCPD
t2[, sortie_det := fcase(top_lscpd_red == T, "c_lscpd",
                         top_lsc_red == T, "b_lsc",
                         ap_sortie_sas == T, "a_ap")]

# table finale (dios mio)
sas_fin <- t2[, .(nm_ecrou_init, lc_sas, 
                  dt_deb_sas, dt_fin_sas, 
                  entree_place_sl, entree_ap_sl,
                  sdef_sas = coalesce(sortie_def_sas, F), ap_sortie_sas = coalesce(ap_sortie_sas, F), 
                  ap_in, ap_while, ap_out, sortie_det, dt_mex_ap, 
                  top_lsc_red, top_lscpd_red)]

# redressement date de fin, pour que date de début d'aménagement = date de sortie (j'aime pas cette idée mais bon...)
sas_fin[, dt_fin_red := as.Date(dt_fin_sas)]
sas_fin[sdef_sas == T & !is.na(sortie_det) & dt_mex_ap %between% .(dt_deb_sas, dt_fin_sas), dt_fin_red := dt_mex_ap]

# [DECEMBRE 2023] : on enlève ceux qui entrent sur une place SL, ou avec un aménagement
sas_fin <- sas_fin[entree_place_sl == 0 & ap_in == 0]

# table à part pour les sassistes qui ne sont pas entrés avec un aménagement tout prêt
sas_ss_ap <- sas_fin[ap_in == F]

# résultats ----
effectif_mens <- function(dt, var_date){
  tab_long <- dt[, uniqueN(nm_ecrou_init), 
                 keyby = .(lc_sas, year(get(var_date)), lubridate::month(get(var_date), label = T, abbr = F))]
  tab_long
}

### qui entrent en SAS ----

#### tout court ----
a1 <- effectif_mens(sas_fin, "dt_deb_sas")[, rename(.SD, tot_entrees = V1)]
#### déjà en AP ? ----
a2 <- effectif_mens(sas_fin[ap_in == T], "dt_deb_sas")[, rename(.SD, tot_entrees_ap = V1)]
#### sans AP ? ----
a3 <- effectif_mens(sas_fin[ap_in == F], "dt_deb_sas")[, rename(.SD, tot_entrees_ss_ap = V1)]

### qui sort de la SAS ----

#### tout court ----
# tous les sassistes
b1 <- effectif_mens(sas_fin[year(dt_fin_red) < 9000], "dt_fin_red")[, rename(.SD, tot_sorties = V1)]
# tous les sassistes, avec "vraie" sortie de détention (ie libération ou continuation/début de l'aménagement de peine)
b2 <- effectif_mens(sas_fin[year(dt_fin_red) < 9000 & sdef_sas == T], "dt_fin_red")[, rename(.SD, tot_sorties_def = V1)]
# en haut + seulement les sassistes qui n'avaient pas d'aménagement à l'entrée 
b3 <- effectif_mens(sas_ss_ap[year(dt_fin_red) < 9000 & sdef_sas == T], "dt_fin_red")[, rename(.SD, tot_sorties_ssap = V1)]

#### avec un AP ----
c1 <- effectif_mens(sas_ss_ap[year(dt_fin_red) < 9000 & sdef_sas == T & sortie_det == "a_ap"], "dt_fin_red")[, rename(.SD, ap_simple = V1)]

#### en LSC ----
c2 <- effectif_mens(sas_ss_ap[year(dt_fin_red) < 9000 & sdef_sas == T & sortie_det == "b_lsc"], "dt_fin_red")[, rename(.SD, ap_lsc = V1)]

#### en LSCPD ----
c3 <- effectif_mens(sas_ss_ap[year(dt_fin_red) < 9000 & sdef_sas == T & sortie_det == "c_lscpd"], "dt_fin_red")[, rename(.SD, ap_lscpd = V1)]


## finalement, je fais les sorties def de tous, pas que ceux entrés sans AP (je sens que c'est compté comme ça, côté IP1...)
d1 <- effectif_mens(sas_fin[year(dt_fin_red) < 9000 & sdef_sas == T & sortie_det == "a_ap"], "dt_fin_red")[, rename(.SD, ap_simple_all = V1)]

d2 <- effectif_mens(sas_fin[year(dt_fin_red) < 9000 & sdef_sas == T & sortie_det == "b_lsc"], "dt_fin_red")[, rename(.SD, ap_lsc_all = V1)]

d3 <- effectif_mens(sas_fin[year(dt_fin_red) < 9000 & sdef_sas == T & sortie_det == "c_lscpd"], "dt_fin_red")[, rename(.SD, ap_lscpd_all = V1)]

## BONUS : parmi les entrants sans AP, ceux qui en obtiennent un en cours/en fin de route
e1 <- effectif_mens(sas_ss_ap[ap_while == T], "dt_mex_ap")[,rename(.SD, ssap_w = V1)]
e2 <- effectif_mens(sas_ss_ap[ap_out == T], "dt_mex_ap")[,rename(.SD, ssap_o = V1)]

# ON COLLE TOUT ####
# on met tous les tableaux dans une liste
liste_tabs <- list(a1, #a2, a3,
                   b1, b2, #b3,
                   # c1, c2, c3,
                   d1, d2, d3)

# on fait la jointure des neuf d'un coup 
tab_sas_complet <- reduce(liste_tabs, full_join) %>% 
  mutate(across(everything(), \(x) replace_na(x, 0))) %>% 
  # très artisanal, à revoir si jamais on répète cette commande 
  filter(year == 2023, lubridate != "octobre")

# *~pivoter, renommer, embellir, supprimer~*
tab_sas0 <- tab_sas_complet %>% 
  pivot_longer(cols = -(lc_sas:lubridate), names_to = "effectifs") %>% 
  pivot_wider(names_from = lubridate, values_from = value, values_fill = 0) %>% 
  mutate(total = rowSums(pick(-c(lc_sas:effectifs)))) %>% 
  mutate(effectifs = case_match(effectifs,
                                "tot_entrees" ~ "1 - Nouveaux entrants en SAS",
                                # "tot_entrees_ap" ~ "1.1 - en aménagement de peine",
                                # "tot_entrees_ss_ap" ~ "1.2 - sans aménagement de peine",
                                "tot_sorties" ~ "2 - Sortants de SAS",
                                "tot_sorties_def" ~ "2.1 - sortants de détention, tous",
                                # "tot_sorties_ssap" ~ "2.2 - sortants de détention, entrants sans AP",
                                # "ap_simple" ~ "2.2.1 - entrants sans AP, sortie aménagement de peine",
                                # "ap_lsc" ~ "2.2.2 - entrants sans AP, sortie LSC classique",
                                # "ap_lscpd" ~ "2.2.3 - entrants sans AP, sortie LSC de plein droit",
                                "ap_simple_all" ~ "2.1.1 - sortie aménagement de peine",
                                "ap_lsc_all" ~ "2.1.2 - sortie LSC classique",
                                "ap_lscpd_all" ~ "2.1.3 - sortie LSC de plein droit"
                                # ,"ssap_w" ~ "1.2.1 - en débutent un durant la détention"
                                # ,"ssap_o" ~ "1.2.2 - en débutent un après la détention"
  )
  ) %>%
  arrange(lc_sas, year, effectifs) %>% 
  rename_with(str_to_title) 

# par SAS
tab_sas <- tab_sas0 %>% 
  split(.$Lc_sas) %>% 
  map(~ select(., -Lc_sas, -Year))

# France entière
tab_sas_nat0 <- tab_sas0 %>% 
  group_by(Year, Effectifs) %>% 
  summarise(across(where(is.numeric), sum), .groups = "drop") %>% 
  select(-Year)

tab_sas_nat <- list("France entière" = tab_sas_nat0)

# ON EXPORTE ----
library(openxlsx)

export <- append(tab_sas_nat, tab_sas)

## Styles ----
### style titre
st_t <- createStyle(textDecoration = "bold", fontSize = 14)

### style noms colonnes
st_h <- createStyle(textDecoration = "bold", fontSize = 11, 
                    halign = "center", valign = "center",
                    border = "TopBottomLeftRight",
                    fgFill = "#374B85", fontColour = "#ffffff",
                    wrapText = TRUE)

## Classeur et mise en forme ----
### création classeur excel ----
deb_tab <- 7

wb <- buildWorkbook(
  export, 
  startRow = deb_tab,
  headerStyle = st_h,
  borders = "all",
  gridLines = FALSE,
  zoom = 80
)

### Marianne, police par défaut du classeur ----
modifyBaseFont(wb, fontName = "Marianne", fontSize = 11)

### nom DISP allongé ----
vec_disp <- str_replace(names(export), "MONTPELLIER", "Villeneuve les Maguelone")
vec_disp <- str_replace_all(str_to_title(vec_disp), "Sas ", "SAS du CP de ") 
vec_disp[vec_disp == "France Entière"] <- 'France entière'

### raccourcir noms onglets DISP ----
names(wb) <- ifelse(str_starts(names(wb), "SAS"), 
                    str_replace(names(wb), "SAS ", "") %>% str_to_title(), 
                    names(wb))

### index cellules "spéciales" pour indentation (...à reformuler)
i_cat <- which(str_starts(tab_sas_nat0$Effectifs, "\\d\\s"))+deb_tab
i_souscat <- which(str_starts(tab_sas_nat0$Effectifs, "\\d\\.\\d\\s"))+deb_tab
i_soussouscat <- which(str_starts(tab_sas_nat0$Effectifs, "\\d\\.\\d\\.\\d"))+deb_tab

### largeur des colonnes ----
### pour tableaux par DISP
# nb: walk() fait la même chose que map(), sauf qu'il ne génère pas de sortie :) (setColWidths = fonction anonyme je crois ? application de la fonction sans création d'objet nécessaire)
walk(seq(length(export)), 
     \(onglet) setColWidths(wb, onglet, cols = 1:ncol(export[[onglet]]), 
                            widths = c(53, rep(10,ncol(export[[onglet]])-1)))
)

### styles cellules ----
## style nombre
# par DISP
walk(seq(length(export)),
     \(onglet) addStyle(wb, onglet, 
                        rows = 8:(nrow(export[[onglet]])+deb_tab), cols = 2:ncol(export[[onglet]]), gridExpand = TRUE,
                        style = createStyle(numFmt = "# ##0_ ;-# ##0\ ", border = "TopBottomLeftRight"))
)

## style effectif
# catégorie (X)
walk(1:length(export), 
     \(onglet) addStyle(wb, onglet, rows = i_cat, cols = 1:ncol(export[[onglet]]), gridExpand = TRUE,
                        style = createStyle(textDecoration = "bold", fgFill = "#e5e9f4"), stack = T))

# sous catégorie (X.X)
walk(1:length(export), 
     \(onglet) addStyle(wb, onglet, rows = i_souscat, cols = 1, 
                        style = createStyle(textDecoration = "italic", indent = 2), stack = T))

# sous sous catégorie (X.X.X)
walk(1:length(export), 
     \(onglet) addStyle(wb, onglet, rows = i_soussouscat, cols = 1, 
                        style = createStyle(indent = 4), stack = T))


### infos tableau ----
## 1) titre
walk(1:length(export),
     \(onglet) writeData(wb, onglet, "Flux d'entrées, de sorties et d'aménagements au sein des SAS", startRow = 1))
# appliquer style titre 
walk(1:length(export),
     \(onglet) addStyle(wb, onglet, rows = 1, cols = 1, style = st_t))

## 2) Champ géo: SAS
walk(1:length(export),
     \(onglet) writeData(wb, onglet, vec_disp[onglet], startRow = 2))

## 3) Date fraicheur
walk(1:length(export),
     \(onglet) writeData(wb, onglet, paste("Données au", dt_max_long), startRow = 3))

## 4) Sources
walk(1:length(export),
     \(onglet) writeData(wb, onglet, "Source : ministère de la Justice/DAP/EX3, Infocentre Pénitentiaire - Vue GIDE-GENESIS", startRow = 4))

## 5) Champ
walk(1:length(export),
     \(onglet) writeData(wb, onglet, "Champ : détenus entrant en SAS, hors places SL et hors aménagement à l'arrivée en SAS", startRow = 5))

## mise en forme des notes - en italique 
walk(1:length(export), 
     \(onglet) addStyle(wb, onglet, rows = 2:5, cols = 1, style = createStyle(textDecoration = "italic")))


## Sauvegarde ----
# sauvegarde classeur dans répertoire de la demande 
export_file <- glue::glue("SAS_{format(dt_max, '%Y%m')}.xlsx") 

saveWorkbook(wb, file = export_file, overwrite = TRUE)

openXL(export_file)
