// Processing names data from REDS

// Goal: Randomly splits data into testing and training datasets. Use the trai-
//		 ning dataset to create P(caste|name) for each of the largest ten low
//		 castes, as well as P(muslim|name) and P(hindu|name). This process is
//		 repeated multiple times (as set by the global "sims"). I vary three pa-
//       rameters in these simulations:
//		 	1. Size of the training dataset = 1 - tgt_shs
//			2. Whether all names or only last names are used
//			3. The method to aggregate predictions at the individual level from 
//			   multiple names.

// State directories:
gl dir = "/Volumes/ECB_backup/Files/Research/caste_pol_ineq"
gl dir_in = "$dir/data/built" // To import caste and name data
gl dir_res = "$dir/quest_access/results" // To store result of simulations


// List of ten castes:
gl jati_list "balmiki baori dhanak dhobi jatav khatik kori musahar other pasi"

gl rel_list "mus oth" // Religions
gl agg avg_ln avg wtavg_ln wtavg post_ln post max_ln max // Aggregation methods

loc tgt_shs 0.975 0.5 0.75 // Share of the dataset used for testing the caste algo.

// Set simulation counter to 1 and total simulations to 100.
loc s = 1
gl sims = 100 // Number of simuation runs, usually 100


clear
// Create temporary dataset storing results as simulations pass. This helps a-
// void losing data if the computer crashes due to the large number of simula-
// tions. Temporary datasets stored not as tempfiles to avoid filling the memo-
// ry of my personal computer.
qui set obs 1
	gen sim = 0
save "$dir_res/temp_results_long.dta", replace

set seed 20220302 // Set seed to replicate previous results when needed.

// Outer loop varies share of data split into the testing dataset:
foreach tgt_sh in `tgt_shs' {
	// Inner loop goes over 100 simulations for each size of the dataset:
	forval s = 1/$sims {

		use "$dir_in/reds_name_caste_mtr_sc.dta", clear
			di "Running SC simulations for training share `tgt_sh': `s' / $sims."
	
			qui keep if inlist(sex, "M", "F")
			qui replace caste_agg = "other" if caste_agg == "other sc"
			
			// Random draws to sample a portion of household heads:
			qui egen tag_id = tag(villageid hlist06)
			qui gen rand = runiform() if tag_id
				qui bys villageid hlist06 : ereplace rand = min(rand)
				qui drop tag_id
			qui bys villageid hlist06: egen nvrand = nvals(rand)
			qui assert nvrand == 1
				qui drop nvrand
			
			loc fullN = _N
			
			preserve
				// Create testing subset dataset:
				qui keep if rand < `tgt_sh'
				drop rand
				qui save "$dir_res/temp_testing", replace
			restore
			
			// Create training subset dataset:
			qui drop if rand < `tgt_sh'
			drop rand

			// Given a training dataset, the following code estimates the proba-
			// bility of having a name given the caste: P(name|caste)
			preserve
				order villageid hlist06 name_type name_ name_hin name_sndx   ///
					  name_hin_sndx sex caste_agg balmiki-pasi
				keep villageid hlist06 name_type name_ name_hin name_sndx    ///
					 name_hin_sndx sex caste_agg balmiki-pasi
				qui egen tag_hin_sndx = tag(villageid hlist06 name_type name_hin_sndx)
				qui keep if tag_hin_sndx
				drop name_-name_sndx tag
				
				qui replace caste_agg = "other" if caste_agg == "other sc"

				// Calculating P(name | caste, sex):
				qui bys caste_agg sex name_hin_sndx : gen xnum = _N
				qui bys caste_agg sex : gen xden = _N
				qui gen hin_sndx_sh_ = xnum / xden
				lab var hin_sndx_sh_ "P(name_`sfx' = name[_n] | caste = caste[_n], sex)"
				// Calculating P(name | sex):
				qui bys name_hin_sndx sex : gen xnum_name = _N
				qui bys sex : gen xden_name = _N
				qui gen hin_sndx_sh = xnum_name / xden_name
				lab var hin_sndx_sh "P(name_hin_sndx = name[_n] | sex)"
				drop x*
				
				// Calculating P(caste | sex):
				foreach var of global jati_list {
					qui bys sex : egen `var'_sh = mean(`var')
					loc proper_caste = proper("`var'")
					lab var `var'_sh "P(caste = `proper_caste' | sex)"
					drop `var'
				}

				drop villageid hlist06 name_type
				qui duplicates drop
				
				// Reshape dataset to be at the name-sex-level to merge with the
				// training dataset.
				qui reshape wide hin_sndx_sh_, i(name_hin_sndx sex) j(caste_agg) string
				
				foreach var of global jati_list {
					loc proper_caste = proper("`var'")
					lab var hin_sndx_sh_`var' "P(name_hin_sndx = name[_n] | caste = `proper_caste', sex)"
				}

				isid name_hin_sndx sex
				qui compress
				qui save "$dir_res/temp_hin_sndx_P_caste", replace
				
				keep sex balmiki_sh-pasi_sh
				qui duplicates drop
				isid sex
				qui save "$dir_res/temp_Pcaste_sex", replace
				
			restore

			sort villageid hlist06 name_type
			
			keep name_ name_hin name_sndx name_hin_sndx villageid hlist06    ///
				 name_type sex
			
			// Create tags at the name, sex level:
			foreach sfx in "" "hin" "hin_sndx" {
				qui egen tag_`sfx' = tag(name_`sfx' sex)
			}
				
			foreach sfx in "" "hin" "hin_sndx" {
				preserve
					qui keep if tag_`sfx'
					qui keep name_`sfx' sex
					qui save "$dir_res/temp_train_`sfx'", replace
				restore
			}
		
		// Comparing predictions to the observed castes in the training dataset:
		use "$dir_res/temp_testing", clear

			sort villageid hlist06 name_type
			
			foreach sfx in "" "hin" "hin_sndx" {
				// Merge only names to create estimates of the share of matched 
				// observations:
				qui merge m:1 name_`sfx' sex using "$dir_res/temp_train_`sfx'", keep(1 3) gen(_m_`sfx')
				// Save share of observations that are matched:
				qui count if _m_`sfx' == 3
				loc sh_m_`sfx' = `r(N)' / _N
				qui bys villageid hlist06 : egen max_m_`sfx' = max(_m_`sfx')
				qui count if max_m_`sfx' == 3
				loc sh_max_m_`sfx' = `r(N)' / _N
			}
			
			// Merge predictions of P(name|caste)
			qui merge m:1 name_hin_sndx sex using "$dir_res/temp_hin_sndx_P_caste", keep(1 3) nogen
			drop balmiki_sh-pasi_sh
			qui merge m:1 sex using "$dir_res/temp_Pcaste_sex", assert(3) nogen
			
			foreach jati of global jati_list {
				cap confirm var hin_sndx_sh_`jati'
				if _rc > 0 {
					gen hin_sndx_sh_`jati' = .
				}
			}
			
			// Aggregating predictions to the individual-level across names:
			
			// Normalized Bayesian updating: This aggregation method incorpora-
			// tes the multiple names for each individuals as independent sig-
			// nals starting from a uniform prior of P(caste):
			// P(caste|name) = P(name|caste)*P(caste)/P(name)
			qui gen _ln = regexm(name_type, "n") // Distinguish last names
			qui sort villageid hlist06 name name_type
			foreach var of global jati_list {
				// Assuming matched names that don't show up in REDS with a cer-
				// tain religion actually have P(name|religion) = 0.
				qui gen xPfillin0 = (_m_hin_sndx == 3 & missing(hin_sndx_sh_`var'))
					qui replace hin_sndx_sh_`var' = 0 if xPfillin0 == 1
				qui gen x`var'_postPr1 = ///
							(hin_sndx_sh_`var' / hin_sndx_sh) // P(name|caste)/P(name)
				qui gen xmiss = missing(x`var'_postPr1) & !xPfillin0
				// Assuming missing name information shouldn't change the prior.
				qui replace x`var'_postPr1 = 1 if missing(x`var'_postPr1)
				
				// Marking observations with all missing values:
				qui bys villageid hlist06 : egen shmiss_`var' = mean(xmiss)
				qui bys villageid hlist06 : egen shfilln0_`var' = mean(xPfillin0)
		
				// Create P(caste|name) for each name signal:
				qui gen `var'_sh_hin_sndx = `var'_sh * (x`var'_postPr1)
			
				// Create separate predictions using last names and all names
				// and aggregate within a household (villageid hlist06):
				foreach ntype in "" "_ln" {
					qui bys villageid hlist06 `ntype': ///
						gen x`var'_postPr`ntype' = `var'_sh * (x`var'_postPr1) if _n == 1
					qui bys villageid hlist06 `ntype': ///
						replace x`var'_postPr`ntype' = x`var'_postPr`ntype'[_n-1] * ///
									(x`var'_postPr1) if _n > 1
					// Replace P(caste|name) with final posterior for last names:
					qui bys villageid hlist06 `ntype': ///
						gen `var'_post`ntype' = x`var'_postPr`ntype'[_N]
				}
				qui drop xmiss xPfillin0
				
				// Replace with missing values P(caste|name) for households with
				// no last names reported:
				qui replace `var'_post_ln = 1000 if _ln == 0
				qui bys villageid hlist06: ereplace `var'_post_ln = min(`var'_post_ln)
				qui replace `var'_post_ln = . if `var'_post_ln == 1000
			}
			
			// Normalize P(caste|name) to add up to 1 across castes, within household:
			qui egen xnorm = rowtotal(*_post)
			qui egen xnorm_ln = rowtotal(*_post_ln)

			foreach var of global jati_list {
				qui replace `var'_post = `var'_post / xnorm
				loc proper_rel = proper("`var'")
				lab var `var'_post "Agg. P(rel = `proper_rel' | names), Bayes' posterior"
				qui replace `var'_post_ln = `var'_post_ln / xnorm_ln
				lab var `var'_post_ln "Only last names, Agg. P(rel = `proper_rel' | names), Bayes' posterior, only ln"
				qui gen x`var'miss = missing(`var'_post)
				qui gen x`var'miss_ln = missing(`var'_post_ln)
				qui gen x`var'miss_P = shmiss_`var' == 1
				
			}
			
			qui drop xnorm*
			
			// Weighted averages: wt = P(name|rel)/P(name)
			foreach var of global jati_list {
				foreach ntype in "" "_ln" {
					qui bys villageid hlist06 `ntype': egen `var'_wtavg`ntype' = wtmean(`var'_sh_hin_sndx), weight(x`var'_postPr1)
				}
				qui replace `var'_wtavg_ln = 1000 if _ln == 0
				qui bys villageid hlist06: ereplace `var'_wtavg_ln = min(`var'_wtavg_ln)
					qui replace `var'_wtavg_ln = . if `var'_wtavg_ln == 1000
			}
			
			qui egen xnorm = rowtotal(*_wtavg)
			qui egen xnorm_ln = rowtotal(*_wtavg_ln)

			foreach var of global jati_list {
				qui replace `var'_wtavg = `var'_wtavg / xnorm
				loc proper_rel = proper("`var'")
				lab var `var'_wtavg "Agg. P(rel = `proper_rel' | names), wtavg."
				qui replace `var'_wtavg_ln = `var'_wtavg_ln / xnorm_ln
				lab var `var'_wtavg_ln "Only last names, Agg. P(rel = `proper_rel' | names), wtavg."
			}

			qui drop xnorm*

			// Simple averages: 
			foreach var of global jati_list {
				foreach ntype in "" "_ln" {
						qui bys villageid hlist06 `ntype': egen `var'_avg`ntype' = mean(`var'_sh_hin_sndx)
				}
				qui replace `var'_avg_ln = 1000 if _ln == 0
				qui bys villageid hlist06: ereplace `var'_avg_ln = min(`var'_avg_ln)
						qui replace `var'_avg_ln = . if `var'_avg_ln == 1000
			}
		
			qui egen xnorm = rowtotal(*_avg)
			qui egen xnorm_ln = rowtotal(*_avg_ln)

			foreach var of global jati_list {
				qui replace `var'_avg = `var'_avg / xnorm
				loc proper_rel = proper("`var'")
				lab var `var'_avg "Agg. P(rel = `proper_rel' | names), avg."
				qui replace `var'_avg_ln = `var'_avg_ln / xnorm
				lab var `var'_avg_ln "Only last names, Agg. P(rel = `proper_rel' | names), avg."
			}
			qui drop xnorm*
		
			// Maximum: 
			foreach var of global jati_list {
				foreach ntype in "" "_ln" {
					qui bys villageid hlist06 `ntype': egen `var'_max`ntype' = max(`var'_sh_hin_sndx)
				}
				qui replace `var'_max_ln = 1000 if _ln == 0
				qui bys villageid hlist06: ereplace `var'_max_ln = min(`var'_max_ln)
				qui replace `var'_max_ln = . if `var'_max_ln == 1000
			}
		
			qui egen xnorm = rowtotal(*_avg)
			qui egen xnorm_ln = rowtotal(*_avg_ln)

			foreach var of global jati_list {
				qui replace `var'_max = `var'_avg / xnorm
				loc proper_rel = proper("`var'")
				lab var `var'_max "Agg. P(rel = `proper_rel' | names), max."
				qui replace `var'_avg_ln = `var'_avg_ln / xnorm
				lab var `var'_max_ln "Only last names, Agg. P(rel = `proper_rel' | names), max."
			}
			qui drop xnorm*

			qui drop x* *sh* _m* //wt*
			qui drop name_type name_hin_sndx name_hin name_ name* _ln
			qui duplicates drop
			qui isid villageid hlist06
			
			// Aggregate results into metrics comparing the true and predicted
			// castes. I use two measures: 1) probability predicted for the co-
			// rrect caste (1 - Type II error) and 2) probability predicted for 
			// each incorrect caste (Type I error):
			foreach agg of global agg { 
				foreach jati of global jati_list {
					qui gen Pr_T_`jati'_`agg' = `jati'_`agg' if caste_agg == "`jati'"
					qui gen Pr_Type1_`jati'_`agg' = `jati'_`agg' if caste_agg != "`jati'"
				}
			}
			
			
			// Aggregating the results by the median across observations and 
			// storing them as locals to save in a separate dataset.
			foreach agg of global agg {
				foreach jati of global jati_list {
					foreach sex in "M" "F" {
						qui count if Pr_T_`jati'_`agg' != . & sex == "`sex'"
						if `r(N)' == 0 {
							loc Pr_T_50_`sex'_`jati'_`agg' = .
						}
						else {
							qui sum Pr_T_`jati'_`agg' if sex == "`sex'", d
							loc Pr_T_50_`sex'_`jati'_`agg' = `r(p50)'
						}
						qui count if Pr_Type1_`jati'_`agg' != . & sex == "`sex'"
						if `r(N)' == 0 {
							loc Pr_Ty1_50_`sex'_`jati'_`agg' = .
						}
						else {
							qui sum Pr_Type1_`jati'_`agg' if sex == "`sex'", d
							loc Pr_Ty1_50_`sex'_`jati'_`agg' = `r(p50)'
						}
					}
				}
			}

			// Store results for each simulation. The end of this do-file aggre-
			// gates the results across simulations:
			preserve
				qui clear
				qui set obs 1
					qui gen sim = `s'
					qui gen socgp = "caste"
					qui gen tgt_test_sh = `tgt_sh'
					qui gen sh_m_ = `sh_m_'
					qui gen sh_m_hin = `sh_m_hin'
					qui gen sh_m_hin_sndx = `sh_m_hin_sndx'
					qui gen sh_max_m_hin_sndx = `sh_max_m_hin_sndx'
					
					foreach jati of global jati_list {
						foreach agg of global agg {
							foreach sex in "M" "F" {
								qui gen Pr_T_50_`sex'_`jati'_`agg' = 		 ///
													`Pr_T_50_`sex'_`jati'_`agg''
								qui gen Pr_Ty1_50_`sex'_`jati'_`agg' =       ///
												  `Pr_Ty1_50_`sex'_`jati'_`agg''
							}
						}
					}
				qui append using "$dir_res/temp_results_long.dta"
				qui save "$dir_res/temp_results_long.dta", replace
			restore
	}
}

// Repeat procedure above for religion, instead of caste:
foreach tgt_sh in `tgt_shs' {

	forval s = 1/$sims {

		use "$dir_in/reds_name_caste_mtr_oc.dta", clear
			di "Running OC simulations for training share `tgt_sh': `s' / $sims."
			
			qui keep if inlist(sex, "M", "F")
			
			// Random draws to sample a portion of household heads:
			qui egen tag_id = tag(villageid hlist06)
			qui gen rand = runiform() if tag_id
				qui bys villageid hlist06 : ereplace rand = min(rand)
				qui drop tag_id
			qui bys villageid hlist06: egen nvrand = nvals(rand)
			qui assert nvrand == 1
				qui drop nvrand
			
			preserve
				qui keep if rand > `tgt_sh'
				drop rand
				qui save "$dir_res/temp_testing", replace
			restore
			
			loc fullN = _N
			qui drop if rand > `tgt_sh'
			drop rand
			loc act_sh = _N / `fullN'
			preserve
				order villageid hlist06 name_type name_ name_hin    ///
					  name_hin_sndx sex muslim
				keep villageid hlist06 name_type name_ name_hin     ///
					 name_hin_sndx sex muslim
				qui egen tag_hin_sndx = tag(villageid hlist06 name_type name_hin_sndx)
				qui keep if tag_hin_sndx
			
				drop name_ name_hin tag
				
				gen count = 1
				egen tag_id = tag(villageid hlist06) // to avoid double-counting
				
				// Calculating P(name | muslim, sex):
				qui bys muslim sex name_hin_sndx : egen xnum = total(count)
				qui bys muslim sex : egen xden = total(count * tag_id)
				qui gen hin_sndx_sh_ = xnum / xden
				lab var hin_sndx_sh_ "P(name_`sfx' = name[_n] | muslim = muslim[_n], sex)"
				
				// Calculating P(name | sex):
				qui bys name_hin_sndx sex : gen xnum_name = _N
				qui bys sex : egen xden_name = total(count * tag_id)
				qui gen hin_sndx_sh = xnum_name / xden_name
				lab var hin_sndx_sh "P(name_hin_sndx = name[_n] | sex)"
				
				// Calculating P(muslim | sex):
				qui gen _sh = xden / xden_name
				qui gen mus_sh = _sh if muslim
					qui bys sex: ereplace mus_sh = min(mus_sh)
					drop _sh
				lab var mus_sh "P(muslim = 1 | sex)"
				drop tag_id x*
				
				// Creating string variable for whether or not the person is muslim. 
				// This is useful to reshape the data into name_hin_sndx-sex level.
				qui gen mus_str = "oth"
					qui replace mus_str = "mus" if muslim
				drop muslim
				
				drop villageid hlist06 name_type
				qui duplicates drop
				qui reshape wide hin_sndx_sh_, i(name_hin_sndx sex) j(mus_str) string

				lab var hin_sndx_sh_mus "P(name_hin_sndx = name[_n] | rel = muslim, sex)"
				lab var hin_sndx_sh_oth "P(name_hin_sndx = name[_n] | rel != muslim, sex)"
			
				isid name_hin_sndx sex
				qui compress
				qui save "$dir_res/temp_hin_sndx_P_mus", replace
				
				keep sex mus_sh
				qui duplicates drop
				qui gen oth_sh = 1 - mus_sh
				isid sex
				qui save "$dir_res/temp_Pmus_sex", replace
				
			restore
			

			sort villageid hlist06 name_type
			keep name_ name_hin name_sndx name_hin_sndx villageid hlist06    ///
				 name_type sex
			
			foreach sfx in "" "hin" "hin_sndx" {
				qui egen tag_`sfx' = tag(name_`sfx' sex)
			}
				
			foreach sfx in "" "hin" "hin_sndx" {
				preserve
					qui keep if tag_`sfx'
					qui keep name_`sfx' sex
					qui save "$dir_res/temp_train_`sfx'", replace
				restore
			}
		
		use "$dir_res/temp_testing", clear

			sort villageid hlist06 name_type
			
			foreach sfx in "" "hin" "hin_sndx" {
				qui merge m:1 name_`sfx' sex using "$dir_res/temp_train_`sfx'", keep(1 3) gen(_m_`sfx')
				qui count if _m_`sfx' == 3
				loc sh_m_`sfx' = `r(N)' / _N
				qui bys villageid hlist06 : egen max_m_`sfx' = max(_m_`sfx')
				qui count if max_m_`sfx' == 3
				loc sh_max_m_`sfx' = `r(N)' / _N
			}
			
			qui merge m:1 name_hin_sndx sex using "$dir_res/temp_hin_sndx_P_mus", keep(1 3) nogen
			
			drop mus_sh 
			qui merge m:1 sex using "$dir_res/temp_Pmus_sex", assert(3) nogen
			
			// Aggregating predictions to the individual-level across names:
			
			// Normalized Bayesian updating:
			qui gen _ln = regexm(name_type, "n")
			qui sort villageid hlist06 name name_type
			foreach var of global rel_list {
				// Assuming matched names that don't show up in REDS with a cer-
				// tain religion actually have P(name|religion) = 0.
				qui gen xPfillin0 = (_m_hin_sndx == 3 & missing(hin_sndx_sh_`var'))
					qui replace hin_sndx_sh_`var' = 0 if xPfillin0 == 1
				qui gen x`var'_postPr1 = ///
							(hin_sndx_sh_`var' / hin_sndx_sh) 
				qui gen xmiss = missing(x`var'_postPr1) & !xPfillin0
				qui replace x`var'_postPr1 = 1 if missing(x`var'_postPr1)
				
				// Marking observations with all missing values:
				qui bys villageid hlist06 : egen shmiss_`var' = mean(xmiss)
				qui bys villageid hlist06 : egen shfilln0_`var' = mean(xPfillin0)
		
				qui gen `var'_sh_hin_sndx = `var'_sh * (x`var'_postPr1)
			
				foreach ntype in "" "_ln" {
					qui bys villageid hlist06 `ntype': ///
						gen x`var'_postPr`ntype' = `var'_sh * (x`var'_postPr1) if _n == 1
					qui bys villageid hlist06 `ntype': ///
						replace x`var'_postPr`ntype' = x`var'_postPr`ntype'[_n-1] * ///
									(x`var'_postPr1) if _n > 1
					qui bys villageid hlist06 `ntype': ///
						gen `var'_post`ntype' = x`var'_postPr`ntype'[_N]
				}
				qui drop xmiss xPfillin0
				
				qui replace `var'_post_ln = 1000 if _ln == 0
				qui bys villageid hlist06: ereplace `var'_post_ln = min(`var'_post_ln)
				qui replace `var'_post_ln = . if `var'_post_ln == 1000
			}
			
			qui egen xnorm = rowtotal(*_post)
			qui egen xnorm_ln = rowtotal(*_post_ln)

			foreach var of global rel_list {
				qui replace `var'_post = `var'_post / xnorm
				loc proper_rel = proper("`var'")
				lab var `var'_post "Agg. P(rel = `proper_rel' | names), Bayes' posterior"
				qui replace `var'_post_ln = `var'_post_ln / xnorm_ln
				lab var `var'_post_ln "Only last names, Agg. P(rel = `proper_rel' | names), Bayes' posterior, only ln"
				qui gen x`var'miss = missing(`var'_post)
				qui gen x`var'miss_ln = missing(`var'_post_ln)
				qui gen x`var'miss_P = shmiss_`var' == 1
				
			}
			
			qui drop xnorm*

			// Weighted averages: wt = P(name|rel)/P(name)
			foreach var of global rel_list {
				foreach ntype in "" "_ln" {
					qui bys villageid hlist06 `ntype': egen `var'_wtavg`ntype' = wtmean(`var'_sh_hin_sndx), weight(x`var'_postPr1)
				}
				qui replace `var'_wtavg_ln = 1000 if _ln == 0
				qui bys villageid hlist06: ereplace `var'_wtavg_ln = min(`var'_wtavg_ln)
					qui replace `var'_wtavg_ln = . if `var'_wtavg_ln == 1000
			}
			
			qui egen xnorm = rowtotal(*_wtavg)
			qui egen xnorm_ln = rowtotal(*_wtavg_ln)

			foreach var of global rel_list {
				qui replace `var'_wtavg = `var'_wtavg / xnorm
				loc proper_rel = proper("`var'")
				lab var `var'_wtavg "Agg. P(rel = `proper_rel' | names), wtavg."
				qui replace `var'_wtavg_ln = `var'_wtavg_ln / xnorm_ln
				lab var `var'_wtavg_ln "Only last names, Agg. P(rel = `proper_rel' | names), wtavg."
			}

			qui drop xnorm*

			// Simple averages: 
			foreach var of global rel_list {
				foreach ntype in "" "_ln" {
						qui bys villageid hlist06 `ntype': egen `var'_avg`ntype' = mean(`var'_sh_hin_sndx)
				}
				qui replace `var'_avg_ln = 1000 if _ln == 0
				qui bys villageid hlist06: ereplace `var'_avg_ln = min(`var'_avg_ln)
						qui replace `var'_avg_ln = . if `var'_avg_ln == 1000
			}
		
			qui egen xnorm = rowtotal(*_avg)
			qui egen xnorm_ln = rowtotal(*_avg_ln)

			foreach var of global rel_list {
				qui replace `var'_avg = `var'_avg / xnorm
				loc proper_rel = proper("`var'")
				lab var `var'_avg "Agg. P(rel = `proper_rel' | names), avg."
				qui replace `var'_avg_ln = `var'_avg_ln / xnorm
				lab var `var'_avg_ln "Only last names, Agg. P(rel = `proper_rel' | names), avg."
			}
			qui drop xnorm*
		
			// Maximum: 
			foreach var of global rel_list {
				foreach ntype in "" "_ln" {
					qui bys villageid hlist06 `ntype': egen `var'_max`ntype' = max(`var'_sh_hin_sndx)
				}
				qui replace `var'_max_ln = 1000 if _ln == 0
				qui bys villageid hlist06: ereplace `var'_max_ln = min(`var'_max_ln)
				qui replace `var'_max_ln = . if `var'_max_ln == 1000
			}
		
			qui egen xnorm = rowtotal(*_avg)
			qui egen xnorm_ln = rowtotal(*_avg_ln)

			foreach var of global rel_list {
				qui replace `var'_max = `var'_avg / xnorm
				loc proper_rel = proper("`var'")
				lab var `var'_max "Agg. P(rel = `proper_rel' | names), max."
				qui replace `var'_avg_ln = `var'_avg_ln / xnorm
				lab var `var'_max_ln "Only last names, Agg. P(rel = `proper_rel' | names), max."
			}
			qui drop xnorm*

			qui drop x* *sh* _m* count //wt*
			qui drop name_type name_hin_sndx name_hin name_ name* _ln
			qui duplicates drop
			qui isid villageid hlist06
			
			qui gen rel_str = ""
				qui replace rel_str = "mus" if muslim
				qui replace rel_str = "oth" if !muslim
			
			foreach agg of global agg { 
				foreach rel of global rel_list {
					qui gen Pr_T_`rel'_`agg' = `rel'_`agg' if rel_str == "`rel'"
					qui gen Pr_Type1_`rel'_`agg' = `rel'_`agg' if rel_str != "`rel'"
				}
			}

			foreach agg of global agg {
				foreach rel of global rel_list {
					foreach sex in "M" "F" {
						qui count if Pr_T_`rel'_`agg' != . & sex == "`sex'"
						if `r(N)' == 0 {
							loc Pr_T_50_`sex'_`rel'_`agg' = .
						}
						else {
							qui sum Pr_T_`rel'_`agg' if sex == "`sex'", d
							loc Pr_T_50_`sex'_`rel'_`agg' = `r(p50)'
						}
						qui count if Pr_Type1_`rel'_`agg' != . & sex == "`sex'"
						if `r(N)' == 0 {
							loc Pr_Ty1_50_`sex'_`rel'_`agg' = .
						}
						else {
							qui sum Pr_Type1_`rel'_`agg' if sex == "`sex'", d
							loc Pr_Ty1_50_`sex'_`rel'_`agg' = `r(p50)'
						}
					}
				}
			}

			preserve
				qui clear
				qui set obs 1
				dis "1"
					qui gen sim = `s'
					qui gen socgp = "religion"
					qui gen tgt_test_sh = `tgt_sh'
					//qui gen act_test_sh = 1 - `act_sh'
					qui gen sh_m_ = `sh_m_'
					qui gen sh_m_hin = `sh_m_hin'
					qui gen sh_m_hin_sndx = `sh_m_hin_sndx'
					qui gen sh_max_m_hin_sndx = `sh_max_m_hin_sndx'
					foreach rel of global rel_list {
						foreach agg of global agg {
							foreach sex in "M" "F" {
								qui gen Pr_T_50_`sex'_`rel'_`agg' = 		 ///
													`Pr_T_50_`sex'_`rel'_`agg''
								qui gen Pr_Ty1_50_`sex'_`rel'_`agg' =       ///
												  `Pr_Ty1_50_`sex'_`rel'_`agg''
							}
						}
					}
				qui append using "$dir_res/temp_results_long.dta"
				qui save "$dir_res/temp_results_long.dta", replace

			restore
	}
}

// Aggregate results across imulations:
use "$dir_res/temp_results_long.dta", clear
	sort tgt_test_sh sim 
	qui keep if sim > 0
	drop sim
	gen socgp_num = 1
		qui replace socgp_num = 2 if socgp == "religion"
	drop socgp
	foreach var of varlist * {
		qui bys tgt_test_sh socgp_num : ereplace `var' = mean(`var')
	}
	gen socgp = "caste"
		qui replace socgp = "religion" if socgp_num == 2
	qui duplicates drop
	drop socgp_num
	order tgt_test_sh socgp, first
	
	// Create stubs for reshape:
	local stubs
	foreach rel in $jati_list $rel_list {
		foreach var in Pr_T_50_M Pr_Ty1_50_M Pr_T_50_F Pr_Ty1_50_F {
			local stubs `stubs' `var'_`rel'_
		}
	}
	
	qui reshape long `stubs', i(tgt_test_sh socgp) j(agg_type) string
	ren P*_ P*
	qui reshape long Pr_T_50_M_ Pr_T_50_F_ Pr_Ty1_50_M_ Pr_Ty1_50_F_,        ///
							   i(tgt_test_sh socgp agg_type) j(group) string
	ren P*_ P*
	order Pr_T_50_M Pr_T_50_F Pr_Ty1_50_M Pr_Ty1_50_F, last
	
	ren socgp soccat
	drop if (soccat == "religion" & !inlist(group, "mus", "oth")) |  ///
			(soccat != "religion" &  inlist(group, "mus", "oth"))

save "$dir_res/reds_test_results.dta", replace





		