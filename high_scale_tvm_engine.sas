/* CONTACT INFORMATION:
                * NAME:      Andrew R Goad
                * LinkedIn:  linkedin.com/in/andrewrgoad
                * Email:     ar.goad@yahoo.com

    TITLE:      TIME VALUE OF MONEY (TVM) CALCULATION - USING US 1-YEAR CMT TREASURY RATES

    OBJECTIVE:
                * Compute TVM on each account's financial_amount from its impact_start_date
                  up through an IMPACT_END_DATE (today + 90 days), compounding annually.

                * On each annual "anniversary" day from impact_start_date, sum the prior
                  365 days of daily interest and add it (annual compounding).

                * On the final IMPACT_END_DATE, sum the "remainder" daily interest (since
                  last anniversary) and add it to the amount.

    INPUTS: TREASURY RATES
            ->work.treasury_1yr_cmt_rates : Daily Treasury constant-maturity (1Y) rate
            -->Holidays and weekends are backfilled to the closest prior rate

            STARTING POPULATION
            ->work.starting_population : Base population with one row per account
            -->variables: account_id, impact_start_date, financial_amount

    PERFORMANCE:
                * Split starting_population into manageable chunks (macro %split_dataset),
                  build "array-ready" inputs per chunk, then process in batches with
                  PROC FCMP arrays to reduce memory pressure.

    NOTE:
                * All compounding is annual using sums of daily interest across periods.

                * Daily rate = (treasury_rate / 365).

                * The last observation for each account_id contains the final amount
                  (original financial_amount + TVM).
*/

/* 1) IMPACT_END_DATE macro assignment
      ->Define the analysis end date as TODAY + 90 days and push it to a macro
        variable &IMPACT_END_DATE in a date literal ('DDMONYYYY'd) form.
*/
DATA _NULL_;
    IMPACT_END_DATE = INTNX('DAY', TODAY(),+90); /* today + 90 days - adjust as necessary*/
    CALL SYMPUT('IMPACT_END_DATE',"' "||PUT(IMPACT_END_DATE,DATE9.)||"'D");
RUN;

/*2) Performance Macros - Adjust per your server environment*/
%let split = 200000; /*Accounts per split dataset created by %split_dataset.*/
%let per_array_loop = 5000; /*Accounts processed in each FCMP loop iteration.
                      ->should be less than &split macro above. recommend 10k or less processed at a time
                      due to memory limitations of our servers.
                      proc fcmp processes in memory*/

/*3) Find earliest impact_start_date in starting_population and store as &start_dt
      -> This narrows Treasury date filtering to the earliest needed date.*/
proc means data=work.starting_population (keep=impact_start_date) noprint;
    var impact_start_date;
    output out= impact_start_date (keep=impact_start_date) min=;
run;

proc sql;
    select CATS("'",put(impact_start_date,date9.),"'d") into :start_dt from impact_start_date;
quit;

/*4) Obtain total obs count for work.starting_population
      -> Used to compute how many split datasets we need.*/
%let dsnid = %sysfunc(open(work.starting_population));
%let nobs=%sysfunc(attrn(&dsnid,nlobs));
%let rc =%sysfunc(close(&dsnid));

/*5) Macro: %split_dataset
      Purpose:
        - Slice starting_population into chunks of size &increment starting at &start.
        - For each chunk, build an "array input" dataset tvm_array<b> with the columns
          and helper flags the FCMP routine expects.
      Key globals set:
        - &d           : Number of chunks created.
        - &g_dataset   : Dataset name (work.starting_population).
        - &g_increment : Chunk size (i.e., &split).
        - &g_total     : Total observation count.
        - &g_a, &g_c   : Begin/end indices for the first chunk.*/
%macro split_dataset(dataset,start,increment,totalobs,ignore);
    /* Proceed only if chunking divides the range cleanly or ignore=Yes */
    %if %sysfunc(mod(&totalobs-&start+1,&increment)) = 0 or &ignore = Yes %then %do;
        %global g_a g_c g_dataset g_increment g_total d;
        %let g_a = %eval(&start);
        %let g_c = %eval(&increment+&start-1);
        %let d = %sysfunc(ceil((&totalobs-&start+1)/&increment));
        %put &d;
        %let g_dataset = &dataset;
        %let g_increment = %eval(&increment);
        %let g_total = %eval(&totalobs);

        %do i = 1 %to 1;
            /* You can parallelize with Grid/MP by increasing this loop */

            /*Inner macro: %array_input(b)
                - For chunk b, build tvm (joined Treasury daily rates to account slice),
                  then derive tvm_array<b> with helper variables:

                  place_indicator:
                    1 = first observation for account_id
                    9 = last observation for account_id
                    0 = observations in-between

                  account_id_ct:
                    Running count of days for the account; its max equals the number of
                    days from impact_start_date to &IMPACT_END_DATE.

                  anniversary:
                    1 when account_id_ct is an exact multiple of 365 (annual boundary),
                    used to trigger annual compounding.

                  daily_int, int_summed:
                    Working columns for interest per day and summed interest, initialized
                    to missing, later populated by PROC FCMP.

                  Also compute last_days:
                    - Number of days between &IMPACT_END_DATE and the most recent annual
                      anniversary.
                    - This controls final remainder compounding.*/
            %macro array_input(b);
                /* Compute row range for this chunk b */
                %let begin = %eval(&g_a + ((&b - 1)*&g_increment));
                %let finish = %eval(&g_c + ((&b - 1)*&g_increment));
                %let total = %eval(&g_total);

                /* Build the joined table "tvm" for the chunk:
                   - Treasury rates filtered between earliest needed (&start_dt) and &IMPACT_END_DATE.
                   - Account slice limited to [begin .. finish] rows.
                   - Ensure treasury_dt >= impact_start_date so we only keep needed days.
                   - Compute daily_rate on the fly (treasury_rate / 365).
                */
                %if &finish le &total %then %do;
                    proc sql;
                        create table tvm (where= (financial_amount > 0)) as
                        select a.*,
                               (a.treasury_rate / 365) as daily_rate,
                               b.*
                        from work.treasury_1yr_cmt_rates (where= (treasury_dt ge &start_dt. and treasury_dt le &impact_end_date.)) as a
                             , (select account_id, impact_start_date, financial_amount
                                from &g_dataset (firstobs=&begin obs=&finish keep = account_id impact_start_date financial_amount)) as b
                        where a.treasury_dt >= b.impact_start_date order by b.account_id, a.treasury_dt;
                    quit;
                %end;
                %else %do;
                    /* If finish goes beyond total, cap obs at &total */
                    proc sql;
                        create table tvm (where= (financial_amount > 0)) as
                        select a.*,
                               (a.treasury_rate / 365) as daily_rate,
                               b.*
                        from work.treasury_1yr_cmt_rates (where= (treasury_dt ge &start_dt. and treasury_dt le &impact_end_date.)) as a
                             , (select account_id, impact_start_date, financial_amount
                                from &g_dataset (firstobs=&begin obs=&total keep = account_id impact_start_date financial_amount)) as b
                        where a.treasury_dt >= b.impact_start_date order by b.account_id, a.treasury_dt;
                    quit;
                %end;

                /* Create the array-friendly dataset with flags and counters.
                   - "by account_id" sets FIRST./LAST.
                   - flags across each account's rows.
                   - account_id_ct increments daily across treasury_dt order.
                   - anniversary flag uses an integer-test:
                     If int(n/365) - (n/365) = 0 => n is an exact multiple of 365. */
                data work.tvm_array&b (keep= account_id place_indicator account_id_ct daily_rate financial_amount anniversary daily_int int_summed);
                    set tvm;
                    by account_id;
                    if first.account_id then place_indicator = 1;
                    else if last.account_id then place_indicator = 9;
                    else place_indicator = 0;
                    if first.account_id then account_id_ct = 0;
                    account_id_ct+1;
                    if int(account_id_ct / 365) - (account_id_ct / 365) = 0 then anniversary = 1;
                    else anniversary = 0;
                    daily_int = .;    /* Will hold per-day interest = amount * daily_rate */
                    int_summed = .;
                    /* Will hold running sums used for annual compounding */
                    length anniversary place_indicator 3.;
                run;

                /* Capture obs count for this chunk into a macro variable &&account&b */
                %global account&b;
                %let dsnid = %sysfunc(open(work.tvm_array&b));
                %let account&b=%sysfunc(attrn(&dsnid,nlobs));
                %let rc =%sysfunc(close(&dsnid));

                /*--- Compute last_days for remainder compounding at &IMPACT_END_DATE ---*/

                /*--- Get latest anniversary rows per account (where anniversary=1) ---*/
                proc sort data= work.tvm_array&b (keep= account_id anniversary account_id_ct where= (anniversary = 1))
                    out= last_anniversary;
                    by account_id descending account_id_ct; /* bring the latest anniversary first */
                run;
                /*--- Deduplicate to one row per account with the latest anniversary ct ---*/
                proc sort data= last_anniversary (drop= anniversary rename= (account_id_ct =last_anniversary_ct)) nodupkey;
                    by account_id;
                run;

                /*--- Merge to compute last_days:
                     -> If no anniversary occurred (short spans <365), last_days=account_id_ct.
                     -> Else last_days = account_id_ct - last_anniversary_ct.*/
                data last_anniversary_ct (keep= account_id last_days);
                    merge work.tvm_array&b last_anniversary;
                    by account_id;
                    if last.account_id;
                    if missing(last_anniversary_ct)=1 then last_days = account_id_ct;
                    else last_days = account_id_ct - last_anniversary_ct;
                run;
                /*--- Attach last_days back to the array dataset ---*/
                data work.tvm_array&b;
                    merge work.tvm_array&b last_anniversary_ct;
                    by account_id;
                run;

                /* Cleanup intermediates for this chunk */
                proc datasets lib=work nolist;
                    delete last_anniversary_ct tvm;
                quit;
                run;
            %mend array_input;
            %array_input(&i);

        %end;
    %end;
    %else %do;
        %put "Total observations divided by increment produces non zero remainder. Ignore field was not marked by Yes";
    %end;
%mend split_dataset;

/* Execute the split to create d chunk(s) with array-ready inputs */
%split_dataset(work.starting_population, 1, &split, &nobs, Yes);

/*6) Macro: %process(b)
     Purpose:
       - For chunk b, run the FCMP array logic in batches of &per_array_loop accounts.
       - The FCMP code reads the batch ("temp") into an array and performs:
         * Carry-forward of compounded amount after each anniversary.
         * Annual compounding: sum of daily_int across the prior 365 days.
         * Final remainder compounding on &IMPACT_END_DATE using last_days.
       - It writes results back into "temp", then extracts the last row (per account)
         as financial_amount_plus_tvm.
       - Array column mapping inside FCMP (dataset 'temp' read into array a[row, col]):
       col1 : daily_rate
       col2 : financial_amount (will be updated with compounded additions)
       col3 : place_indicator (1 first, 9 last, 0 middle)
       col4 : account_id_ct (day counter within the account)
       col5 : account_id (numeric)
       col6 : anniversary (1 on annual boundary, else 0)
       col7 : daily_int (per-day interest = col2 * col1)
       col8 : int_summed (running sum used to add annual/remainder compounding)
       col9 : last_days (days since last anniversary up to &IMPACT_END_DATE)*/

%macro process(b);
    %let x = 0;             /* lower bound of account_id to process in this pass */
    %let y = &per_array_loop;
    /* upper bound of account_id to process in this pass */
    %let z = 1;
    /* output partition counter for this chunk */

    /* Loop until all accounts in tvm_array<b> have been processed */
    %do %while (%eval(&x) < %eval(&&account&b +1));
        /* Add +1 so the loop doesn't stop one batch early;
           ensures the final range is processed even with 'account_id > &x' */

        /*Selects the accounts to process through the proc fcmp array next.
          Macro variable &sqlobs is created that represents
          number of observations*/
        proc sql;
            create table temp as select * from work.tvm_array&b (where= (account_id > &x and
                                                                          account_id le &y));
        quit;

        /*======================== PROC FCMP CORE ========================
          - Read 'temp' into array a.
          - Iterate rows (y=1..&sqlobs), applying the compounding rules:

            Rule A (Carry-forward after anniversary):
              If not the first row (place_indicator != 1), carry forward any previous
              compounded addition (int_summed from prior row) into financial_amount.
            Rule B (On anniversary rows):
              * Compute daily_int for the day (amount * daily_rate).
              * Set int_summed = today's daily_int, then add the previous 364 days'
                daily_int values to it (annual compounding).
              * If this anniversary day is also the last row for the account,
                add int_summed into financial_amount.
            Rule C (On last row when NOT an anniversary):
              * Sum the final remainder days (last_days) of daily_int into int_summed.
              * Add int_summed into financial_amount.

            Rule D (All other rows, non-anniversary, non-last):
              * Only compute daily_int for the day.
          - Write the updated array back to 'temp'.
        ================================================================*/

        proc fcmp;
            array a[&sqlobs., 9] / nosymbols;
            /* Define 2D array */
            rc1 = read_array("temp", a);
            /* Read dataset -> array */

            do y = 1 to &sqlobs;
                /* Loop through rows */

                /* Rule A: carry-forward of compounded amount after an anniversary day */
                if a[y,3] ne 1 then do;
                    /* not first row in account */
                    if a[y-1,8] ne .
                    then a[y,2] = a[y-1,2] + a[y-1,8];
                    else                  a[y,2] = a[y-1,2];
                end;

                /* Rule B: annual anniversary day -> sum prior 365 daily_ints */
                if a[y,6] = 1 then do;
                    a[y,7] = a[y,2] * a[y,1];           /* daily_int today */
                    a[y,8] = a[y,7];
                    /* start annual sum */

                    do i = 1 to 364;
                        /* add prior 364 days */
                        a[y,8] = a[y,8] + a[y - i,7];
                    end;

                    /* If last obs for the account occurs here, add the annual compounding */
                    if a[y,9] = 0 and a[y,3] = 9 then do;
                        a[y,2] = a[y,2] + a[y,8];
                    end;
                end;

                /* Rule C: not an anniversary AND last row -> remainder compounding */
                else if a[y,3] = 9 then do;
                    a[y,7] = a[y,2] * a[y,1];           /* daily_int today */
                    a[y,8] = a[y,7];
                    /* start remainder sum */

                    /* Sum across the remainder last_days - 1 prior days */
                    do i = 1 to a[y,9] - 1;
                        a[y,8] = a[y,8] + a[y - i,7];
                    end;

                    a[y,2] = a[y,2] + a[y,8];
                    /* final amount for account */
                end;
                /* Rule D: plain day (no anniversary, not last) -> store only daily_int */
                else do;
                    a[y,7] = a[y,2] * a[y,1];
                end;

            end;

            res = write_array('temp', a);           /* Write array -> dataset */
        quit;

        /* Keep only the last observation per account and rename back to meaningful names.
           financial_amount_plus_tvm = final amount (original + compounded interest).
           Note: 'a2' and 'a5' are the array-backed positions for amount and account_id.
        */
        data work.tvm_array&b._&z;
            set temp (keep= a2 a5 rename= (a2 = financial_amount_plus_tvm a5 = account_id));
            by account_id;
            if last.account_id;
            label account_id = "account_id";
            label financial_amount_plus_tvm = "financial_amount_plus_tvm";
        run;

        /* Advance batch window and output partition counter */
        %let x = %eval(&x + &per_array_loop);
        %let y = %eval(&y + &per_array_loop);
        %let z = %eval(&z + 1);
    %end;
    /* end while over batches */

    /* Combine the outputs from all batches for this chunk b */
    data work.tvm_array_complete&b;
        set work.tvm_array&b._1-work.tvm_array&b._%eval(&z -1);
        by account_id;
    run;

    /* Cleanup chunk-level intermediates */
    proc datasets lib=work nolist;
        delete tvm_array&b tvm_array&b._1-tvm_array&b._%eval(&z -1) temp;
    quit;
    run;
%mend process;

/* 7) Driver macro to run %process over chunks
      - Currently set to 1 (matching the earlier loop).
      - Increase if you create
        multiple chunks and want to process them in series in this session.
*/
%macro run_process;
    %do i=1 %to 1; /* Can scale/parallelize depending on &d */
        %process(&i);
    %end;
%mend run_process;
%run_process;
