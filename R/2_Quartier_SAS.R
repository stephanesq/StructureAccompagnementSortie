
### ON REPART DE ZERO ###

## Sur la topo des SAS : c'est relativement fiable et correct depuis mi-septembre 2023,
## avec la màj de la topo du CP de Marseille. 
## Les lignes précédentes sont à prendre avec des pincettes. Je préconise très fortement
## de filtrer sur FLAG_VALIDITE == "Y", qui donne la ligne la plus récente. Tant pis pour
## l'historique.

# packages ----
pacman::p_load(tidyverse, arrow, data.table, janitor, haven)

# paramètres ----
fraicheur <- 2310 # dernière mensuelle avec données dispo
dt_mens <- ym(fraicheur)

an <- 2023
mois <- 7
dt_obs <- ym(paste(an, mois))  # date pour tester photo des écroués en SAS à une date donnée (eg dernière mensuelle)

dt_max <- ym(2310) # AAMM date dernière extraction des bases écoles 
dt_max_long <- format(dt_max, "%d %B %Y")

# chemin ------
path = paste0(here::here(),"/Donnees/")
path_dwh = "~/Documents/Recherche/3_Evaluation/_DATA/INFPENIT/"
path_ref = "~/Documents/Recherche/3_Evaluation/_DATA/Referentiel/"

# memory.limit(size = 18000)

# 1. Liste des SAS ------

## 1.1 liste des établissements historisés (ref_etab) ----
## libellé ouvert si plusieurs
ref_etab <- read_sas(paste0(path_ref, "ref_etab_historisee.sas7bdat")) |>  
  clean_names() |> 
  arrange(cd_etablissement,desc(dt_fermeture),desc(dt_disp)) |> 
    group_by(cd_etablissement) |> 
    slice(1) |> 
  select(cd_etablissement, type_etab, lc_etab)

## 1.2. liste des SAS renseignées dans SRJ (pour date d'application/date d'ouverture) ----
# reprise des codes du SRJ qui ont été modifiées dans GENESIS

# liste_sas <- read_sas(paste0(path_ref, "t_dwh_lib_structure.sas7bdat")) |> 
#   clean_names() |> 
#   filter(nm_type_structure == 3729) |> 
#   mutate(lib_sas = paste0(lb_structure_1, lb_structure_2),
#          .keep = "unused"
#          ) |> 
#   rename(c("lc_sas" = "lc_structure",
#            "c_ori_est" = "cd_orig_structure",
#            "nu_est" = "nm_structure",
#            "d_app" = "dt_application",
#            "c_ori_est_rat" = "cd_orig_structure_rattach", 
#            "nu_est_rat" = "nm_structure_rattach" 
#          )) |> 
#   select(id_structure, c_ori_est, nu_est, lc_sas, lib_sas, d_app, id_structure_rattach, c_ori_est_rat, nu_est_rat)
# 
# write_parquet(liste_sas,paste0(path,"Export/liste_sas.parquet"))


# sur les dates d'application : les SAS de Marseille, Poitiers, Bordeaux, Longuenesse et Aix
# ont toutes une date d'application au 01/05/2022, même si elles étaient ouvertes avant (en raison
# de l'arrêté les reconnaissant n'étant pas publié à leurs mises en service). Pour
# les autres, la date d'application correspond bien à la date de mise en service de la SAS.
srj_sas_raw <- read_parquet(paste0(path, "Export/liste_sas.parquet")) %>% clean_names()

srj_sas <- srj_sas_raw %>% 
  mutate(cd_etab_sas = str_c(sprintf("%03s", c_ori_est), sprintf("%05s", nu_est)),
         cd_etab_rat = str_c(sprintf("%03s", c_ori_est_rat), sprintf("%05s", nu_est_rat)), # établissement de rattachement
         .keep = "unused") %>% #"unused" retains only the columns not used in ... to create new columns. 
  select(cd_etab_sas, lc_sas, d_app, cd_etab_rat)

rm(srj_sas_raw)
# write.csv2(srj_sas,"srj_sas.csv")

## 1.3. Info métiers SAS ----
# récupérer info métiers sur ouverture SAS : date ouverture ; places ; places dédiées SAS ; + taille QSL si existe
srj_suivi_sas_dap <- readxl::read_xlsx("Documents/Suivi_SAS_SRJ.xlsx") |> 
  clean_names() |> 
  select(dt_ouverture, operation, cd_etab_sas, places_prevues, places_prevues_sas, qsl) |> 
  mutate(neuve = if_else(operation=="Neuve",1,0),
         cd_etab_sas = sprintf("%08s", as.character(cd_etab_sas)),
         .keep = "unused")

#ajout des infos
srj_sas <- srj_sas |> 
  left_join(srj_suivi_sas_dap)

rm(srj_suivi_sas_dap)

# 2. Topographie SAS (cellules) ----
## 2.1. Identifiants des SAS à partir de t_dwh_h_cellule ----
## Rappel cd_categ_admin (REG_UGC_10 de l'IP) : 
##  3 parties, type de quartiers + type de cellules + type de détenu
#REG_UGC_10_1 Type de quartier
#   Si le type d'établissement est 'MA', 'CD', 'MC', 'CSL' ou 'EPM', alors CD_CATEG_ADMIN commence par le code type établissement.
#   Sinon, si le type d'établissement est 'CP' ou 'CPA':
#     Si le code type hébergement est 'EPSN', alors CD_CATEG_ADMIN commence par 'QEPSN'.
#     Sinon, CD_CATEG_ADMIN commence par le code type de quartier (champ TYPE_QUARTIER de la source). 
#     Si la valeur est 'ND', elle est insérée sans changement et les traitements sont arrêtés.
#REG_UGC_10_2 Type de cellule
#     Si la catégorie administrative n'est pas 'CSL', 'QEPSN' ou 'ND':
#       Si le code type hébergement est 'CNO' ou 'CNE', alors CD_CATEG_ADMIN est complété par 'CNO' ou 'CNE'.
#       Si le code type hébergement est 'SMPR', alors CD_CATEG_ADMIN est complété par 'SMP'.
#       Si le code type hébergement est 'Semi-Liberté' et la catégorie administrative n'est pas 'QCSL', alors CD_CATEG_ADMIN est complété par 'SL'.
#       Sinon, si le flag indicateur de Semi-liberté (FL_SL) vaut 1 et la catégorie administrative n'est pas 'QCSL', alors CD_CATEG_ADMIN est complété par 'SL'.
#REG_UGC_10_3 Type de détenu (H/F/Mineur)
#     Si la catégorie administrative n'est pas 'ND' et le type d’établissement n’est pas 'EPM':
#       Si le flag quartier mineur (FL_QM) ou le flag indicateur de Place Mineurs (FL_MINEUR) vaut 1, alors CD_CATEG_ADMIN est complété par 'M'.
#     Si la catégorie administrative n'est pas 'ND':
#       Si le flag indicateur de Places Femmes (FL_FEMME) vaut 1, alors CD_CATEG_ADMIN est complété par 'F'.
#       Sinon, CD_CATEG_ADMIN est complété par 'H'.
### 2.1.1. Réduire cette table à un lien cellule/quartier 
### Redressements :
### RED0 type_quartier redressé 
###   si pas etab simple
### RED1 Flags à NA si non-renseignés (2 initialement)
###   RED1.1 si cd_type_hebergement==SL => fl_sl ==1
### RED2 Fusion  des lignes identiques en fonction de capa_theo,fl_femme, fl_mineur, fl_sl,statut_ugc,lc_code,cd_categ_admin
### RED3.1 Indic indisponible (fl_indisp) à partir du statut_ugc AI ou I 
### RED3.2 Si inoccupable, alors capa = 0
if(!exists("cellule")){
  cellule <- open_dataset(paste0(path_dwh, "t_dwh_h_cellule.parquet")) |> 
    collect() |> 
    clean_names() |> 
    select(-capa_normes_europe,-capa_ope,
           -dt_modification,-id_audit,
           -effectif_present,-effectif_theo_affecte,-nb_lits,-effectif_absent, -effectif_sl_absent,
           -fl_fumeur,-fl_pmr,-fl_qm,
           -id_ugc_histo,-type) |> 
    mutate(across(starts_with("fl_"), ~ if_else(.==2,NA, as.integer(.)))) |> #RED1
    mutate(across(where(is.character), ~ if_else( . %in% c("NA","(ND)","(NF)","(NR)"), NA, as.factor(.))))   |> 
    mutate(fl_speciale = if_else(cd_type_hebergement %in% c("DISC","ISOL","NURS","QCP","QPR","SMPR","UDV","UHSA","UHSI","UVF"),1,0), #identifier des places sans hébergement / dépendent pas de quartier
           fl_sl=if_else(cd_type_hebergement == "SL"| #
                           (cd_type_etab == "CSL" & fl_speciale == 0) | #centre de SL     
                           (str_detect(lc_code,"SL") & fl_speciale == 0) #mention SL dans le nom sauf si info incohérente (discipline)
                            ,1
                            ,fl_sl),
           fl_indisp = if_else(statut_ugc %in% c("AI","I"),1,0), #RED3.1
           capa_theo = if_else(fl_indisp == 1,0, capa_theo) #RED3.2
           ) |> 
    select(-statut_ugc) |> 
    left_join(ref_etab) 
}

### 2.1.3. Réduire le nombre de lignes
### RED1 : pour les flags on vérifie s'il y a un changement de valeur qui se maintient (0,1,1 ou 0,1,0)
### on regarde les modifs pour un nombre réduit de colonnes
### RED2 : on ne garde que les observations avec des changements par id_ugc
### RED3 : on attribue date de fin à la ligne suivante
cellule <- data.table(cellule)
setkey(cellule,id_ugc)

  test <- cellule[,
                  .(date_debut = min(date_debut),
                    date_fin = max(date_fin),
                    n = .N
                    ),
                  by = .(id_ugc,capa_theo,fl_femme, fl_mineur, fl_sl,fl_indisp,lc_code,cd_categ_admin, cd_etablissement)]
  #comptage des doublons
  test <- test[ ,n := .N #comptage ligne
    ,by = .(id_ugc)]
  #modif si doublon et que information paraît saisie APRES
  setorder(test,id_ugc,date_debut)

  ln_structure<- read_sas(paste0(path_ref, "t_dwh_lib_structure.sas7bdat")) 
  ln_type_quartier_detail <- read_sas(paste0(path_ref, "t_dwh_lib_categ_admin.sas7bdat")) 
  
#RED1 : récupère lead1 et lead2  
col_remplies <- c("capa_theo","fl_femme", "fl_mineur", "fl_sl")
##lead1
test <- test[n>1
               ,paste0(col_remplies, "_lead1") := 
                    lapply(.SD, shift, n=1L, type = "lead") #applique la fonction shift sur les colonnes
               ,by = .(id_ugc)
               ,.SDcols = col_remplies]
##lead2
test <- test[n>1 #filtre pour doublons
             ,paste0(col_remplies, "_lead2") := #modifie nom pour chaque colonne
               lapply(.SD, shift, n=2L, type = "lead") #applique la fonction shift sur les colonnes
             #et modifie leur nom
             ,by = .(id_ugc)
             ,.SDcols = col_remplies] #traitement que pour colonnes listées
##redressement
test2 <- test[n>1
               ,.SD := fifelse(
                 .SD == 0 &
                 (paste0(col_remplies, "_lead1") == 1 & paste0(col_remplies, "_lead2") == 1 )
                 | (paste0(col_remplies, "_lead1") == 1 & is.na(paste0(col_remplies, "_lead2")) ) 
                 ,paste0(col_remplies, "_lead1")
                 ,col_remplies )
               ,by = .(id_ugc)
             ,.SDcols = col_remplies]
##lead2         
  


test |> summarise(n = n(), dist_ugc = n_distinct(id_ugc))

## focus SAS MARSEILLE (places manquantes - LE QSL) ---- 
# redressement du QSL en SAS
cellule$type_quartier_red <- ifelse(cellule$lc_etab == "CP MARSEILLE" & cellule$type_quartier == "QCSL", 
                                    "SAS", cellule$type_quartier)

## vérif capacités ----
cellule %>% 
  filter(flag_validite == "Y", type_quartier == "SAS") %>% 
  group_by(lc_etab) %>% summarise(sum(capa_ope))

cellule %>% 
  filter(flag_validite == "Y", type_quartier_red == "SAS") %>% 
  group_by(lc_etab) %>% summarise(sum(capa_ope))

## places SL en SAS
cellule %>% 
  filter(flag_validite == "Y", type_quartier_red == "SAS", cd_type_hebergement == 'SL') %>% 
  group_by(lc_etab) %>% summarise(sum(capa_ope))

## réduction aux UGC SAS ----
# on réduit à tout ce qui réfère aux SAS, UGC la plus "récente" (flag_validite = "Y")
# flag_validite : essentiel, car sans lui on retrouve des erreurs de topo d'avant le chantier des grandes vacances
# avec ça, on est sûr que les ugc sont bien les SAS à la date d'extraction de t_dwh_h_cellule
cell_sas <- cellule %>% 
  filter(
    flag_validite == "Y", # filtre sur la ligne la plus récente => PLUS FIABLE
    # statut_ugc == "AD", # toutes UGC qui étaient opérationnelles à un moment ou un autre
    type_quartier_red == "SAS" 
  ) %>% 
  mutate(sas_sl = fifelse(cd_type_hebergement == "SL", T, F)) %>% # places SL dans les SAS
  distinct(id_ugc, sas_sl, cd_etablissement, lc_etab)

l_sas <- cell_sas %>% 
  # on joint à la table du SRJ, pour avoir nom de la SAS + date de début d'application dans SRJ
  left_join(srj_sas, by = join_by(cd_etablissement == cd_etab_rat)) %>% 
  distinct(id_ugc, sas_sl, cd_etablissement, lc_etab, lc_sas, d_app) %>% 
  as.data.table()

# on récupère les situ_penit des personnes qui sont passées par les SAS ----
situ_cols <- c('nm_ecrou_init', 'nm_ecrou_courant', 'dt_debut_situ_penit', 'dt_fin_situ_penit', 
               'cd_etablissement','id_ugc_ref', 'id_ugc_ref_histo', 
               'cd_categ_admin', 'top_ecroue',
               'cd_type_amenagement', 'cd_amenagement_peine', 'cd_motif_hebergement',
               'dt_suspsl', 'dt_debut_exec', 'top_lsc', 'top_heberge',
               'fl_statut_semi_liberte',
               'top_sortie_def', 'top_evade')

if(!exists("situ_penit")){
  situ_penit  <- read_parquet(str_glue("{path_dwh}t_dwh_h_situ_penit.parquet"),
                              col_select = toupper(situ_cols)) %>% 
    clean_names() %>% 
    mutate(across(where(is.character), \(x) na_if(x, "NA"))) %>% 
    # mutate(across(starts_with("dt_"), as.Date)) %>% 
    as.data.table()
}

# contrôle sur les premières apparitions des id_ugc...
situ_penit[id_ugc_ref > -3, .(min_dt = min(dt_debut_situ_penit), max_dt = max(dt_fin_situ_penit))]
situ_penit[id_ugc_ref_histo > -3, .(min_dt = min(dt_debut_situ_penit), max_dt = max(dt_fin_situ_penit))]
# tout s'explique (presque). la variable id_ugc n'existe que depuis janvier 2023, d'où l'impossibilité de trouver des observations avant 2023 en fonction de la cellule. CD_CATEG_ADMIN C PARTI 

# top_lsc en numérique
situ_penit[, top_lsc := as.numeric(top_lsc)]

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
