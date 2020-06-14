* This file replicates figure 1 of 
* "Data can make justice systems more just //
*   But only if court records are free and accessible to the public"

* To replicate, set file to the folder in which this file lives
*cd "/PATH/TO/FOLDER/WITH/IFP-DATA-&-SCRIPT/"


set more off
set matsize 5000

clear


*import data

	import delimited "figure1_data.csv", encoding(ISO-8859-1)
	

* calculate the difference between judge's grant rate and grant rate of other judges on the same court
	
	* not enough judges in Mariana Islands for intra-district comparison!
	drop if court=="nmid"

	gen judge_b=.
	gen judge_l=.
	gen judge_h=.

	* separately by court
	levelsof(court), local(courts)
	foreach c in `courts'{

		di "Court `c'"
		
		gen grant_c = grant if court=="`c'"
		gen judge_c = judge if court=="`c'"

		* judge j vs judge not-j
		levelsof(judge_c), local(judges)	
		foreach j in `judges'{
			gen j=judge!=`j'
			ttest grant_c, by(j)
			replace judge_b = r(mu_1)-r(mu_2) if judge==`j'
			replace judge_l = (r(mu_1) - r(mu_2)) - invt(r(df_t), 0.975)*r(se) if judge==`j'
			replace judge_h = (r(mu_1) - r(mu_2)) + invt(r(df_t), 0.975)*r(se) if judge==`j'
			drop j
		}
		drop judge_c
		drop grant_c
	}


* number of applications that judge decided
	egen judge_n=count(grant),by(judge)
	by judge, sort: keep if _n==1

	
* statistically significantly different from zero at 95 percent confidence
	gen sig95 = sign(judge_h)==sign(judge_l)

	
* graph judges with >=35 obs
	keep if judge_n>=35
	gsort judge_b
	gen judge_rank=100*_n/_N

	
* percent significantly different from zero at 95 confidence
	tab sig95

	
* figure 1
	#delimit;
	twoway 
	(rcap judge_l judge_h judge_rank if sig==0, lcolor(ltblue) msize(zero) horiz)
	(rcap judge_l judge_h judge_rank if sig==1, lcolor(midblue) msize(zero) horiz)
	(scatter  judge_rank judge_b if sig==0, mcolor(ltblue) msize(vsmall) msymbol(circle) )
	(scatter  judge_rank judge_b if sig==1, mcolor(blue) msize(vsmall) msymbol(circle))
	, 
	xline(0, lcolor(black))
	xlabel(-.75(.25).75, format(%02.1f))
	ylabel(, format(%02.0f))
	legend(off)
	ytitle("Percentile Rank of Judge (Increasingly Likely to Waive Court Fees)")
	xtitle("Likelihood that Judge Waives Court Fees minus" "Likelihood that Other Judges" "in the Same District Waive Fees")
	graphregion(color(white))
	xsize(4)
	ysize(6.472);
	#delimit cr

	graph export figure1.pdf, replace

