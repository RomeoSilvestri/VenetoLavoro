# VenetoLavoro
 
The objective of this project is to establish a mapping between the identification codes of Italian municipalities, dating back to the inception of the Kingdom of Italy, and their current counterparts. Over time, municipalities have undergone various transformations, leading to changes in their identification codes due to:
1) Municipality Suppression
2) Name Changes
3) Administrative and Territorial Adjustments
To achieve this, a Python script has been developed to analyze and map old municipalities to their current equivalents, utilizing datasets provided by ISTAT. Subsequently, for municipalities that couldn't be directly mapped, an additional layer of complexity was addressed. A similarity algorithm based on Levenshtein and Jaccard distances was implemented. This step aims to associate municipalities that may have undergone slight name variations compared to the reference data provided by ISTAT but still correspond to the same geographical entity.
