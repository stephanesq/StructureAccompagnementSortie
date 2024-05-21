---
editor_options: 
  markdown: 
    wrap: 72
---

# Evaluation des structures d’accompagnement vers la sortie (SAS)

## A FAIRE

#### Données à récupérer

-   UGC des détenus : extraire affectation mutation plutôt que
    situ_penit

-   Incidents et sanctions

#### Traitements à finir

-   Quartier : identifier CNE et ESPN (aujourd'hui dans AUT) et quid UAT
    ?
-   SAS/CPA -\> récupérer dans SRJ

## Plan pénitentiaire (site [ministere](https://www.justice.gouv.fr/plan-15-000-places-prison))

Sur les 15 000 places, le plan prévoit la construction de 21 structures
d’accompagnement vers la sortie (SAS), soit 2 000 places
opérationnelles. Les SAS proposent un régime de détention adapté pour
des personnes condamnées, afin de les préparer au retour à la vie
civile. Elles sont identifiées par le service de probation et
d’insertion ainsi que l’établissement pénitentiaire selon plusieurs
critères :

-   un reliquat de peine égal ou inférieur à deux ans ;
-   un risque d’évasion faible ;
-   un besoin d’accompagnement soutenu ;
-   leur capacité à s’adapter à la vie en collectivité.

Situées à mi-chemin entre le milieu ouvert et le milieu carcéral, les
SAS favorisent l’autonomisation et la responsabilisation des personnes
détenues, et leur offrent une relative liberté de circulation. Elles
sont situées en milieu urbain pour faciliter l’intervention de
partenaires extérieurs (Pôle emploi, services sociaux, Éducation
nationale, associations…) et le maintien des liens sociaux et familiaux.
En facilitant ainsi la réinsertion des personnes détenues, les SAS sont
un moyen efficace de lutter contre la récidive.

Rappel de la doctrine

-   Toute condamnation à une peine de quartier disciplinaire ferme
    entraîne un retour en détention ordinaire

-   Caractéristiques des éligibles :

    -   Personnes en situation régulière (ou régularisables),

    -   Sans troubles somatiques ou psychiatriques lourds non stabilisés
        – notamment dans l’optique de pouvoir bénéficier de permissions
        de sortir.

    -   Privilégier des profils du territoire, ayant vocation à demeurer
        dans le département à l’issue de leur peine (objectif de
        réinsertion).

    -   Prioritairement les personnes détenues en maison d’arrêt.

## Retour de [l'OIP](https://oip.org/analyse/structures-daccompagnement-vers-la-sortie-de-la-theorie-aux-pratiques/)

-   Doctrine claire après la mise en oeuvre de certains SAS donc des
    particularités locales, par exemple

    -   Affectation possible sans consentement (refus par la SAS de
        Poitiers)

    -   Avis des JAP sur l'affectation (Poitiers)

    -   Priorité des détenus de MA en question

    -   Cadre des permissions de sorties

# Questions évaluatives

### **Question1 - Améliorer le recours aux aménagements de peine**

-   Porte sur : l'efficacité de la politique (adéquation des effets au
    regard des objectifs)

-   Jugée efficace si : le taux d'aménagement de peine en SAS est
    supérieur à celui hors SAS

-   Difficultés :

    -   Evolution avant/après
    -   Certaines SAS créées d'autres issues de reconversion de QPA ou
        de QSL

### Question2 -**Faire bénéficier les aménagements à un public éloigné de ces dispositifs**

-   Jugée efficace si : des publics ne bénéficiant pas d'aménagement
    habituellement sont orientés en SAS

-   Porte sur : le déploiement et mise en œuvre de la politique publique

### **Question3 - Faciliter les permisssions de sortie**

-   Jugée efficace si : les permissions de sorties des personnes en SAS
    sont supérieurs à celles hors SAS 

-   Porte sur : le déploiement et mise en œuvre de la politique publique

### Autres questions posées

-   Désencombrement des MA ?

-   SAS "au mérite" ?

-   SAS seulement après MA ?

# Organisation du programme de travail

### Autres idées

-   Qualité de l'aménagement (jusqu'au bout ou échec)

-   Nb d'examens avant aménagement accordé

### Indicateurs à produire

Aujourd’hui, les données suivantes remontent via des collectes
manuelles : - Le nombre de sortants de détention (= levées d’écrou
libération) et qui viennent d’une SAS ; - Parmi les personnes ci-dessus,
o Le nombre de ceux à qui on a accordé un aménagement de peine ; o Le
nombre de ceux à qui on a accordé une LSC.

Ces indicateurs sont déclinés par établissement pénitentiaire.
Remarques : - Impossible de repérer la SAS de Marseille à partir de
Genesis (mal renseigné) ; - Il faudra inclure toutes les SAS qui
remontent dans Genesis et pas seulement celles qui sont dans les
remontées manuelles.

### Comment faire pour les produire

On pourra utiliser : - La table T_DWH_H_SITU_PENIT, qui contient des
infos sur les aménagements de peine et sur les UGC ; - Les travaux de
Kanto Fiaferana sur la topographie, qui doivent permettre de repérer les
SAS.

### Incertitudes

Deux incertitudes : - L’identification des SAS dans Genesis est-elle
suffisamment propre ? - L’identification des aménagements de peine dans
Genesis est-elle suffisamment propre ?

### Difficultés modélisation

-   Variations

    -   Implantation varie dans le temps (staggered did ?)

    -   Organisation locale

    -   QPA en SAS vs. nouvelles SAS

    -   CP avec SAS vs. établissement non-mixte

-   GEstion des places SL
