
### ON REPART DE ZERO ###

## Sur la topo des SAS : c'est relativement fiable et correct depuis mi-septembre 2023,
## avec la màj de la topo du CP de Marseille. 
## Les lignes précédentes sont à prendre avec des pincettes. Je préconise très fortement
## de filtrer sur FLAG_VALIDITE == "Y", qui donne la ligne la plus récente. Tant pis pour
## l'historique.

# packages ----
pacman::p_load(tidyverse, arrow, data.table, janitor, haven)

# paramètres ----

# chemin ------
path = paste0(here::here(),"/Donnees/")
path_data = "~/Documents/Recherche/3_Evaluation/_DATA/"
path_dwh = "~/Documents/Recherche/3_Evaluation/_DATA/INFPENIT/"
path_ref = "~/Documents/Recherche/3_Evaluation/_DATA/Referentiel/"
path_ref_ip = "~/Documents/Recherche/3_Evaluation/_DATA/Referentiel/"
path_capacite = paste0(path,"AUTRES/Places/")

#sur site
# path = "L:/SDEX/EX3/_EVALUATION_POLITIQUES_PENITENTIAIRES/COMMANDES/2023-06 - SAS - IP1/Donnees/"
# path_dwh = "L:/SDEX/EX3/_ANALYSES_ET_ETUDES/TABLES_DE_DONNEES/R_import/"
# path_ref = "L:/SDEX/EX3/_ANALYSES_ET_ETUDES/TABLES_DE_DONNEES/tables_infpenit/"
# path_ref_ip = "L:/SDEX/EX3/_ANALYSES_ET_ETUDES/TABLES_DE_DONNEES/tables_IP/"
# path_capacite = "L:/SDEX/EX3/_ANALYSES_ET_ETUDES/TABLES_DE_DONNEES/Tables_Places_ope/"


# memory.limit(size = 18000)



# Récupérer établissement puis ajouter caractéristiques selon types de détenu accueillis (SP2)
# Identifier code SRJ des structures SAS
# Associer cette liste avec date ouverture (redressée) à l'établissement

## 1.1 liste des établissements historisés (ref_etab) + capacite ----
### 1.1.1. dernier libellé ouvert si plusieurs -----
ref_etab <- read_sas(paste0(path_ref_ip, "ref_etab_historisee.sas7bdat")) |>  
  clean_names() |> 
  arrange(cd_etablissement,desc(dt_fermeture),desc(dt_disp)) |> 
    group_by(cd_etablissement) |> 
    slice(1) |> 
  select(cd_etablissement, type_etab, lc_etab, dt_fermeture)

### 1.1.2. Liste places ope (SP2) ------
histo_capa_etab <- openxlsx::read.xlsx(paste0(path_capacite,"capa_ope_sp2_histo.xlsx")) |> 
  clean_names() |> 
  select(-disp,-etablissement,-lc_etab,-lc_disp,-lc_spip,-type_etab) |> 
  mutate(dt_mois = janitor::excel_numeric_to_date(dt_mois))

### liste etab avec places femmes
histo_etab_femme <- histo_capa_etab |> filter(places_theo_f > 0) |> select(cd_etablissement) |> distinct()
histo_etab_mineurs <- histo_capa_etab |> filter(places_ope_min > 0) |> select(cd_etablissement) |> distinct()
histo_etab_sl <- histo_capa_etab |> filter(places_sl > 0) |> select(cd_etablissement) |> distinct()
### année minimal de renseignement
histo_capa_annee_min <- min(year(histo_capa_etab$dt_mois))

### 1.1.3. Etablissement à caractéristiques ----
### 
ref_etab <- ref_etab |> 
  mutate(fl_qf = if_else(cd_etablissement %in% histo_etab_femme$cd_etablissement, 1,0),
         fl_qm = if_else(cd_etablissement %in% histo_etab_mineurs$cd_etablissement, 1,0),
         fl_qsl = if_else(cd_etablissement %in% histo_etab_sl$cd_etablissement, 1,0)) |> 
  mutate(across(starts_with("fl_"), ~ ifelse(year(dt_fermeture) < histo_capa_annee_min, NA, .))) #capa renseigné à partir de 


### 1.1.4. Export ------
write_parquet(ref_etab,
              paste0(path,"Export/ref_etab_fl.parquet"),
              compression = "zstd")

rm(histo_capa_etab,histo_etab_femme,histo_etab_mineurs,histo_etab_sl)

## 1.2. liste des SAS renseignées dans SRJ (pour date d'application/date d'ouverture) ----
###  1.2.1 Code du SRJ  modifiées dans GENESIS -----

srj_sas <- read_sas(paste0(path_ref, "t_dwh_lib_structure.sas7bdat")) |>
  clean_names() |>
  filter(nm_type_structure == 3729) |>
  mutate(lib_sas = paste0(lb_structure_1, lb_structure_2),
         .keep = "unused"
         ) |>
  rename(c("lc_sas" = "lc_structure",
           "c_ori_est" = "cd_orig_structure",
           "nu_est" = "nm_structure",
           "d_app" = "dt_application",
           "c_ori_est_rat" = "cd_orig_structure_rattach",
           "nu_est_rat" = "nm_structure_rattach"
         )) |>
  select(id_structure, c_ori_est, nu_est, lc_sas, lib_sas, d_app, id_structure_rattach, c_ori_est_rat, nu_est_rat)

### 1.2.2. Normalisation des codes -----
# sur les dates d'application : les SAS de Marseille, Poitiers, Bordeaux, Longuenesse et Aix
# ont toutes une date d'application au 01/05/2022, même si elles étaient ouvertes avant (en raison
# de l'arrêté les reconnaissant n'étant pas publié à leurs mises en service). Pour
# les autres, la date d'application correspond bien à la date de mise en service de la SAS.

srj_sas <- srj_sas %>% 
  mutate(cd_etab_sas = str_c(sprintf("%03s", c_ori_est), sprintf("%05s", nu_est)),
         cd_etab_rat = str_c(sprintf("%03s", c_ori_est_rat), sprintf("%05s", nu_est_rat)), # établissement de rattachement
         .keep = "unused") %>% #"unused" retains only the columns not used in ... to create new columns. 
  select(cd_etab_sas, lc_sas, d_app, cd_etab_rat)


### 1.2.3. Info métiers SAS ----
# récupérer info métiers sur ouverture SAS : date ouverture ; places ; places dédiées SAS ; + taille QSL si existe

srj_suivi_sas_dap <- readxl::read_xlsx("Documents/Suivi_SAS_SRJ.xlsx") |> 
  clean_names() |> 
  select(dt_ouverture, operation, cd_etab_sas, places_prevues, places_prevues_sas, qsl) |> 
  mutate(neuve = if_else(operation=="Neuve",1,0),
         cd_etab_sas = sprintf("%08s", as.character(cd_etab_sas)),
         places_prevues_sas = as.integer(places_prevues_sas),
         .keep = "unused")

#ajout des infos
srj_sas <- srj_sas |> 
  left_join(srj_suivi_sas_dap)

rm(srj_suivi_sas_dap)

### Export
write_parquet(srj_sas,
              paste0(path,"Export/liste_sas.parquet"),
              compression = "zstd")

srj_sas <- read_parquet(paste0(path,"Export/liste_sas.parquet"))


# 2. Topographie SAS (cellules) ----
# 
## 2.1. Retravail t_dwh_h_cellule ----

## Rappel cd_categ_admin (REG_UGC_10 de l'IP) : 
##  3 parties, type de quartiers + type de cellules (SL/SMPR/..) + type de détenu
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

###2.1.1. Retravail table de paramétrage ------
### A partir des capacités remontées par SP2
### Questions en suspens : ARRIVANT ?
### Cellules hors capacité :
###   Quartiers/Etatblissement "autres" (cd_type_quartier_red == "AUT"): 
###     Déja présents : UHSI/UHSA
###     Ajouts : NURS
###   Situés dans des quartiers/etab : 
###     Déjà présents :
###     Ajouts : NURS
### Type de détenus :
###   Déjà présents : H/F + M 
###   Ajouts : PMR
###
lib_categ_admin <- read_sas(paste0(path_ref, "t_dwh_lib_categ_admin.sas7bdat")) |>
  clean_names() |>
  filter(!str_detect(cd_categ_admin,"CNO") & #Supprime mention CNO (ex nom CNE) 
           !(cd_categ_admin %in% c("(ND)","(NF)","(NR)")) & #ou pas renseigné
           !str_detect(cd_categ_admin,"^EPSN") #ou Quartier EPSN
  ) |> 
  select(-id_audit,-id_categ_admin, -lb_categ_admin) |>
  # Création des flags manquants
  mutate(fl_epsn = if_else(str_detect(cd_categ_admin,"EPSN"), 1, 0),
         fl_uhsi = if_else(str_detect(cd_categ_admin,"UHSI"), 1, 0),
         fl_uhsa = if_else(str_detect(cd_categ_admin,"UHSA"), 1, 0),
         fl_cne = if_else(str_detect(cd_categ_admin,"CNE"), 1, 0),
         fl_CP = if_else(substr(cd_categ_admin,1,1) == "Q"|cd_type_quartier=="SAS", 1,0),
         fl_femme = if_else(str_detect(cd_categ_admin,"F$"), 1, 0),
         fl_mineur = if_else(str_detect(cd_categ_admin,"MF$|MH$|UHSAM|UHSIM|EPM"), 1, 0),
         fl_semiliberte = if_else(str_detect(cd_categ_admin,"SL"), 1, 0),
         fl_cprou = if_else(str_detect(cd_categ_admin,"CPROU"), 1, 0)
  ) |>
  # flag en NA pour les catégories AUTRES
  mutate(fl_CP = if_else(cd_type_quartier=="AUT",NA,fl_CP),
         fl_semiliberte = if_else(cd_type_quartier=="AUT",NA,fl_semiliberte)
  ) |> 
  # Adaptation des noms des variables
  rename("cd_categ_admin_red" = "cd_categ_admin",
         "cd_type_quartier_red" = "cd_type_quartier",
         "fl_sl" = "fl_semiliberte")

openxlsx::write.xlsx(lib_categ_admin, paste0(path_ref, "lib_categ_admin_nvl.xlsx"))

### Pb doublon ?
pb <- lib_categ_admin |> 
  group_by(cd_type_quartier_red,fl_femme,fl_mineur, fl_sl, fl_smpr, fl_epsn, fl_uhsi, fl_uhsa, fl_cne, fl_CP) |> 
  mutate(n=n()) |> 
  ungroup() |> 
  arrange(desc(n),cd_type_quartier_red,fl_femme,fl_mineur, fl_sl, fl_smpr, fl_epsn, fl_uhsi, fl_uhsa, fl_cne, fl_CP)


### 2.1.2. Recalcul cellules -----
### Redressements :
### RED1 Flags à NA si non-renseignés (2 initialement)
### RED2 Etablissement avec quartiers femme ou mineur connu  
### RED3.1 Indic indisponible (fl_indisp) à partir du statut_ugc AI ou I
### RED4 Capacité théorique 
###   RED4.1 Si inoccupable, alors capa = 0
###   RED4.2 Capa théorique >= capa ope max de l'ugc
### RED5 Recalcul cd_type_hebergement
###   RED5.1 cd_type_quartier_etab : calculé selon type étab non-mixte (pas CP) sinon NA 
###   RED5.2 cd_type_quartier_lib_ugc : selon libellé ugc (défaut MA/QMA)
###   RED5.3 fl_hors_capa = cellule hors capacité (détenus liés à une cellule "normale")
###   RED5.4 cd_type_quartier_red = 
###               si fl_hors_capa = 1 -> AUT
###               si cd_type_quartier_etab est renseigné
###               si cd_type_quartier (le quartier dont dépend l'ugc) est renseigné
###               sinon cd_type_quartier_lib_ugc
###   RED5.3 Création des flags à partir de cd_type_hebergement ou le libellé UGC
###     RED5.3.1 fl_EPSN : EPSN que si Fresnes  (MA ou CP, 00101675,00637851)
###     RED5.3.2 fl_sl :
###     RED5.3.3 fl_femme A AMELIORER : selon les lettres/chiffres ensuite
###          fl_femme ==1 : si cellule avec "QF" et établissements accueillant des femmes
###     RED5.3.4 fl_mineur A AMELIORER 
###          fl_mineur ==1 : si cellule avec "QF" et établissements accueillant des femmes
###     
### RED6 refonte table paramétrage avec fl_cp + type de cellules
cellule <- open_dataset(paste0(path_dwh, "t_dwh_h_cellule.parquet")) |> 
    collect() |> 
    clean_names() |> 
    select(-capa_normes_europe,
           -dt_modification,-id_audit,
           -cd_type_etab,
           -nm_type_quartie,-type_quartier,
           -effectif_present,-effectif_theo_affecte,-nb_lits,-effectif_absent, -effectif_sl_absent,
           -fl_fumeur,-fl_pmr,-fl_qm,
           -id_ugc_histo,-type) |> 
    mutate(across(starts_with("fl_"), ~ if_else(.==2,NA, as.double(.)))) |> #RED1
    mutate(across(where(is.character), ~ if_else( . %in% c("NA","(ND)","(NF)","(NR)"), NA, as.factor(.))))   |> 
    mutate(fl_indisp = if_else(statut_ugc %in% c("AI","I") | str_detect(lc_code,"_F$"),1,0) #RED3
           ) |> 
    select(-statut_ugc)

#RED2 modif selon types de places et informations sur places dispo (ref_etab et capa de SP2)
cellule <- cellule |> 
  left_join(read_parquet(paste0(path,"Export/ref_etab_fl.parquet")) |> 
              select(-dt_fermeture)
  )
  
#### RED3 Modif capa théorique ------
cellule <- cellule |> 
  group_by(id_ugc) |> 
  mutate(capa_ope_max = max(capa_ope, na.rm = T)) |> 
  ungroup() |> 
  mutate(capa_theo = case_when(fl_indisp == 1 ~ 0, #RED4.1
                               capa_ope_max > capa_theo ~ capa_ope_max, #RED4.2
                               .default = capa_theo)) |> 
  select(-capa_ope_max)
  
#### RED4 Recalcul cd_type_quartier si renseigné pour id_ugc ----
#privilégie d'abord les nouvelles infos et sinon les plus anciennes (mais fonctionne pas ouf, exemple 83652)
cellule <- cellule |> 
  group_by(id_ugc) |> 
  arrange(date_debut) |> 
  fill(cd_type_quartier, .direction = c("updown")) |> 
  ungroup() 
  
#### RED5 Recalcul éléments cd_categ_admin ------
cellule <- cellule |> 
  mutate(cd_type_quartier_etab = case_when(type_etab == "CP" ~ NA,
                                           type_etab == "CSL" ~ "CSL/QSL",
                                           type_etab == "EPM" ~ "EPM",
                                           .default = paste0(type_etab,"/Q",type_etab)), #RED5.1
         cd_type_quartier_lib_ugc = case_when(str_detect(lc_code,"SAS") ~ "SAS",
                                              str_detect(lc_code,"QPA") ~ "CPA/QPA",
                                              str_detect(lc_code,"MC") ~ "MC/QMC",
                                              str_detect(lc_code,"CD") ~ "CD/QCD",
                                              str_detect(lc_code,"MA") ~ "MA/QMA",
                                              str_detect(lc_code,"SL") ~ "CSL/QSL",
                                              str_detect(lc_code,"SMPR|EPSN|UHSI|UHSA|CNE") ~ "AUT",
                                              .default = NA), #RED5.2
         fl_hors_capa = if_else(cd_type_hebergement %in% c("DISC","ISOL","NURS","QCP","QPR","UDV","UHSA","UHSI","UVF")|
                                  str_detect(lc_code,"DISC|ISOL|NURS|QCP|QPR|UDV|UHSA|UHSI|UVF") #SMPR ?
                                ,1
                                ,0), #identifier des places sans hébergement / dépendent pas de quartier
         cd_type_quartier_red = case_when(fl_hors_capa == 1 ~ "AUT", #hors capa ou
                                          (cd_type_hebergement == "EPSN"|str_detect(lc_code,"EPSN"))  & 
                                            cd_etablissement %in% c("00101675","00637851") ~ "AUT", #RED5.3.1 fl_EPSN  par cohérence avec paramétrage
                                          is.na(cd_type_quartier_etab) & is.na(cd_type_quartier) ~ cd_type_quartier_lib_ugc, 
                                          is.na(cd_type_quartier_etab) ~ cd_type_quartier, 
                                          .default = cd_type_quartier_etab) #RED5.3
  ) |> 
  #Reprise flags
  mutate(# flag type de cellules
    fl_smpr = if_else(str_detect(lc_code,"SMPR")|cd_type_hebergement == "SMPR", 1, 0),
    fl_epsn = if_else(
      (str_detect(lc_code,"EPSN")|cd_type_hebergement == "EPSN") & 
        cd_etablissement %in% c("00101675","00637851")
      , 1
      , 0), #RED5.3.1 fl_EPSN 
    fl_uhsi = if_else(str_detect(lc_code,"UHSI")|cd_type_hebergement == "UHSI", 1, 0),
    fl_uhsa = if_else(str_detect(lc_code,"UHSA")|cd_type_hebergement == "UHSA", 1, 0),
    fl_qd = if_else(str_detect(lc_code,"DISC")|cd_type_hebergement == "DISC", 1, 0),
    fl_qi = if_else(str_detect(lc_code,"ISOL")|cd_type_hebergement == "ISOL", 1, 0),
    fl_nurs = if_else(str_detect(lc_code,"NURS")|cd_type_hebergement == "NURS", 1, 0),
    fl_uvf = if_else(str_detect(lc_code,"UVF")|cd_type_hebergement == "UVF", 1, 0),
    fl_qcp = if_else(str_detect(lc_code,"QCP")|cd_type_hebergement == "QCP", 1, 0),
    fl_qpr = if_else(str_detect(lc_code,"QPR")|cd_type_hebergement == "QPR", 1, 0),
    fl_udv = if_else(str_detect(lc_code,"UDV")|cd_type_hebergement == "UDV", 1, 0),
    fl_cne = if_else(str_detect(lc_code,"CNE")|cd_type_hebergement == "CNE", 1, 0), 
    fl_uat = if_else(str_detect(lc_code,"UAT")|cd_type_hebergement == "UAT", 1, 0), 
    fl_arri = if_else(str_detect(lc_code,"QA")|cd_type_hebergement == "ARRI", 1, 0), 
    fl_CP = if_else(type_etab == "CP" & !is.na(type_etab),1,0),
    fl_cprou = if_else(str_detect(lc_code,"CPRO")|cd_type_hebergement == "CPUR", 1, 0),
    ### traitement spécifique fl_sl
    fl_sl=if_else(cd_type_hebergement == "SL"| cd_type_quartier_red == "CSL/QSL"|str_detect(lc_code,"SL") 
                  ,1
                  ,fl_sl), #RED5.3.2 fl_sl 
    ### flage sur types de détenus
    fl_femme = if_else(str_detect(lc_code,"QF") & fl_qf == 1 ,1,fl_femme), #RED1.3
    fl_mineur = if_else(
      (str_detect(lc_code,"QM") & fl_qm == 1) | cd_type_quartier_red == "EPM"
      ,1
      ,fl_mineur) #RED1.3
  ) |> 
  select(-fl_qf,-fl_qm, -fl_qsl) |> 
  #Remplace NA par 0
  mutate(across(starts_with("fl_"), ~ if_else(is.na(.),0,.)))  #NA to 0 
    
####RED6 jointure avec la table de paramétrage -----
cellule <- cellule |> 
  #supprime var calcul intermédiaires
  # select(-cd_type_quartier_etab,-cd_type_quartier_lib_ugc) |> 
  # left_join(lib_categ_admin
  #           ) |> 
  # cd_categ_admin_red pour les modalités non renseignées
  mutate(cd_categ_admin_quartier_0 = case_when((cd_type_hebergement == "EPSN"|str_detect(lc_code,"EPSN"))  & 
                                               cd_etablissement %in% c("00101675","00637851") ~ "MA/QMA", #RED5.3.1 fl_EPSN  par cohérence avec paramétrage
                                             cd_type_quartier_red == "AUT" & !is.na(cd_type_quartier_etab) ~ cd_type_quartier_etab,
                                             cd_type_quartier_red == "AUT" & !is.na(cd_type_quartier) ~ cd_type_quartier,
                                             cd_type_quartier_red == "AUT" & !is.na(cd_type_quartier_lib_ugc) ~ cd_type_quartier_lib_ugc,
                                             cd_type_quartier_red == "AUT"~ "AUT", 
                                             .default = cd_type_quartier_red),
         cd_categ_admin_quartier = case_when(fl_CP == 1 & 
                                                 str_detect(cd_categ_admin_quartier_0, "^MA|^MC|^CD") ~ paste0("Q",substr(cd_categ_admin_quartier_0,1,2)),
                                               fl_CP == 1 & 
                                                 str_detect(cd_categ_admin_quartier_0, "^CSL|^CPA") ~ paste0("Q",substr(cd_categ_admin_quartier_0,2,3)),
                                               str_detect(cd_categ_admin_quartier_0, "^MA|^MC|^CD") ~ substr(cd_categ_admin_quartier_0,1,2),
                                               str_detect(cd_categ_admin_quartier_0, "^CSL|^CPA") ~ substr(cd_categ_admin_quartier_0,1,3),
                                               .default = cd_categ_admin_quartier_0),
         cd_categ_admin_place = case_when(fl_sl ==1 ~ "SL",
                                          fl_epsn ==1 ~ "EPSN",
                                          fl_smpr ==1 ~ "SMPR",
                                          fl_uhsi ==1 ~ "UHSI",
                                          fl_uhsa ==1 ~ "UHSA",
                                          fl_cne ==1 ~ "CNE",
                                          fl_uat ==1 ~ "UAT",
                                          fl_cprou ==1 ~ "CPROU",
                                          fl_qd == 1 ~ "DISC",
                                          fl_qi == 1 ~ "ISOL",
                                          fl_nurs == 1 ~ "NURS",
                                          fl_uvf == 1 ~ "UVF",
                                          fl_qcp ==1 ~ "QCP",
                                          fl_qpr ==1 ~ "QPR",
                                          fl_udv ==1 ~ "UDV",
                                          fl_arri ==1 ~ "ARRI"
                                          .default = "NORM"),
         cd_categ_admin_detenu = case_when(fl_mineur == 1 & fl_femme == 1 ~ "MF",
                                           fl_mineur == 1 & fl_femme == 0  ~ "MH",
                                           fl_femme == 1 ~ "F",
                                           .default = "H"),
         cd_categ_admin_red = paste(cd_categ_admin_quartier,cd_categ_admin_place, cd_categ_admin_detenu, sep = "_")
  )  
  

### 2.1.3. Synthèse redressement -----
# resume_ecart <- cellule |> 
#   mutate(difference = case_when(is.na(cd_categ_admin) ~ "3_nvl_info",
#                                 cd_categ_admin == cd_categ_admin_red ~ "1_mm_info",
#                                 .default = "2_diff_info")
#   ) |> 
#   group_by(difference) |> 
#   summarise(n=n())
# writexl::write_xlsx(resume_ecart,  paste0(path,"Export/resume_ecart.xlsx"))

tbl_detail_ecart <- cellule |> 
  group_by(cd_categ_admin,cd_categ_admin_red) |> 
  summarise(n=n()) |> 
  ungroup() |> 
  arrange(desc(n))
writexl::write_xlsx(tbl_detail_ecart, paste0(path,"Export/tbl_detail_ecart.xlsx"))

# test_pb <- cellule %>%  filter(is.na(cd_categ_admin_red) & cd_categ_admin == "EPMSLH")

### 2.1.4. Export ------
cellule <- data.table(cellule)
setindexv(cellule,c("id_ugc")) ### Crée index
setorder(cellule, id_ugc, date_debut) ### Ordonne
write_parquet(cellule, 
              paste0(path,"Export/t_dwh_h_cellule_red.parquet"),
              compression = "zstd")

## 2.2. Réduire le nombre de lignes -----
### RED1 : on attribue date de fin à la ligne suivante
### RED2 : on ne garde que les observations avec des changements par id_ugc

### 2.2.1. Import et data.table ----
cellule_red <- read_parquet(paste0(path,"Export/t_dwh_h_cellule_red.parquet"))

cellule_red <- data.table(cellule_red)
setkey(cellule_red,id_ugc)

### 2.2.2. RED1 : date situ
# modif var de date : première ou fin de la précédente
setorder(cellule_red,id_ugc,date_debut)

cellule_red <- cellule_red[order(id_ugc, date_debut)]
cellule_red[, `:=`(
  date_situ_ugc = fcoalesce(
    shift(date_fin, type = "lead"),
    date_debut)
), 
by = id_ugc]  

# nettoyage
cellule_red <- cellule_red[!is.na(cd_categ_admin_red),]
cellule_red <- cellule_red[,
                           `:=`(
                             date_debut = NULL,
                             date_fin = NULL
                           )]

cellule_red <- cellule_red[,
                  .(date_situ_ugc = min(date_situ_ugc)),
                  by = .(id_ugc,lc_code,capa_theo,fl_indisp,fl_hors_capa,cd_categ_admin_red, cd_etablissement, flag_validite)]
#comptage des doublons
cellule_red <- cellule_red[ ,`:=`( 
                n=.N #comptage ligne
                ),
    ,by = .(id_ugc)]

#suppr cellule
rm(cellule)

##2.3. Lien avec etab  -----
### 2.3.1. Ref_etab ----
ref_etab <- open_dataset(paste0(path,"Export/ref_etab_fl.parquet")) |> 
  select(cd_etablissement, lc_etab, dt_fermeture) |> 
  collect()

cellule_red_etab <- merge(cellule_red,
                      ref_etab)

#suppr cellule
rm(cellule_red)

### 2.3.2. Verif cohérence -----
#verif place théoriques
verif_capa_theo <- cellule_red_etab[flag_validite == "Y" & fl_indisp==0,
                         .(capa_theo = sum(capa_theo)),
                         by = .(cd_etablissement,lc_etab,cd_categ_admin_red)]
#verif places sas
verif_capa_theo_sas <- cellule_red_etab[flag_validite == "Y" & fl_indisp==0 & str_detect(cd_categ_admin_red,"SAS_NORM"),
                         .(capa_theo = sum(capa_theo)),
                         by = .(cd_etablissement,lc_etab,cd_categ_admin_red)]

### 2.3.3. Export -----
### 
cellule_red_etab <- data.table(cellule_red_etab)
setindexv(cellule_red_etab,c("id_ugc")) ### Crée index
setorder(cellule_red_etab, id_ugc, date_situ_ugc) ### Ordonne
write_parquet(cellule_red_etab, 
              paste0(path,"Export/cellule_etab.parquet"),
              compression = "zstd")

# haven::write_dta(cellule_red_etab, 
#                  paste0(path,"Export/cellule_etab.dta"),
#                  version = 14)


## focus SAS MARSEILLE (places manquantes - LE QSL) ---- 
# redressement du QSL en SAS
cellule$type_quartier_red <- ifelse(cellule$lc_etab == "CP MARSEILLE" & cellule$type_quartier == "QCSL", 
                                    "SAS", cellule$type_quartier)


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
