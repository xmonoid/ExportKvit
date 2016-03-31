
call lcmccb.pcm_kvee_load_notmkd_csv(&pdat, &pleskgesk, &pdb_lesk, &pnot_empty, &use_filter);

select  bd_lesk,
        postal,
        upper(a.addressshort) addressshort,
        DBM_NAME,
        '' " ",
        Company,
        Inn,
        AccountNum,
        BankName,
        AccountNumOff,
        Bik,
        Month,
        Period,
        Date1,
        Date2,
        Phone,
        Email,
        OrgAddress,
        Barcode,
        Ls,
        Fio,
        Address,
        Count,
        Dpokaz,
        Npokaz,
        Srasch,
        FSumType,
        Dolg,
        Oplata,
        OldPokaz1,
        OldPokaz2,
        OldPokaz3,
        OldPokaz4,
        Pokaz1,
        Pokaz2,
        Pokaz3,
        Pokaz4,
        KoefTr1,
        KoefTr2,
        KoefTr3,
        KoefTr4,
        Kwt1,
        Kwt2,
        Kwt3,
        Kwt4,
        Tarif1,
        Tarif2,
        Tarif3,
        Tarif4,
        Sum1,
        Sum2,
        Sum3,
        Sum4,
        Sum,
        to_char(round(lcmccb.fcm_to_number(RKwt1), 2)) RKwt1,
        to_char(round(lcmccb.fcm_to_number(RKwt2), 2)) RKwt2,
        to_char(round(lcmccb.fcm_to_number(RKwt3), 2)) RKwt3,
        to_char(round(lcmccb.fcm_to_number(RKwt4), 2)) RKwt4,
        to_char(round(lcmccb.fcm_to_number(RKwt), 2)) RKwt,
        RSum1,
        RSum2,
        RSum3,
        RSum4,
        Rsum,
        Total1,
        Total2,
        Total3,
        Total4,
        Total,
        DebtInTitle,
        DebtIn,
        TotalPay,
        PrePay,
        DebtIn1,
        DebtIn2,
        DebtIn3,
        DebtIn4,
        DebtIn5,
        DebtLaw1,
        DebtLaw2,
        DebtLaw3,
        DebtLaw,
        Service1,
        Service2,
        Service3,
        Service,
        CTarifTitle,
        CTarifDate1,
        CTarifDate2,
        VTarifDate1,
        VTarifDate2,
        CTarif1,
        CTarif2,
        VTarif1,
        VTarif2,
        DCTarif1,
        DCTarif2,
        NCTarif1,
        NCTarif2,
        DVTarif1,
        DVTarif2,
        NVTarif1,
        NVTarif2,
        FInfo1,
        Info1,
        Info2,
        FInfo3,
        Info3,
        rownum KolKv,
        PayType,
        Npuch,
        KolKom,
        TINF,
        DATEPOKAZ,
        OZPU,
        CDAY,
        CNIGHT,
        DOLZHNIK
   from (select k.*,
                trim(pr.postal) as postal
           from lcmccb.CM_KVEE_NOTMKD_CSV k,
                rusadm.ci_bill         b,
                rusadm.ci_acct         ac,
                rusadm.ci_prem         pr
          where pdat = &pdat
            and k.bill_id = b.bill_id
            and b.acct_id = ac.acct_id
            and ac.mailing_prem_id = pr.prem_id
            and (&blank_unk = '-1' 
                or
                &blank_unk = '0' and trim(k.ls) is null
                or
                &blank_unk = '1' and trim(k.ls) is not null)
            and ((&use_filter != '1'
                and &mkd_id = '-1'
                and leskgesk = &pleskgesk
                and bd_lesk = &pdb_lesk
                 or nvl(&use_filter, '1') = '1')
            or (nvl(&mkd_id, '-1') = '-1'
                and &use_filter != '1'
                and leskgesk = &pleskgesk
                and bd_lesk = &pdb_lesk
                 or &mkd_id != '-1'
                and k.bill_id in (select bs.bill_id
                                    from rusadm.ci_bseg bs
                                   where trunc(bs.end_dt, 'mm') = &pdat
                                     and bs.bseg_stat_flg = 50
                                     and exists (select null
                                                   from rusadm.ci_prem  pr
                                                  where pr.prem_id = bs.prem_id
                                                    and pr.prnt_prem_id = &mkd_id))))
          order by bd_lesk,
                   postal,
                   upper(k.addressshort),
                   upper(k.address3),
                   to_number(regexp_replace(k.address2,'[^[[:digit:]]]*')),
                   upper(k.address2),
                   to_number(regexp_replace(k.address4,'[^[[:digit:]]]*')),
                   upper(k.address4)) a 
  where ((a.leskgesk, a.bd_lesk) in
                (select distinct trim(a.cis_division),
                        trim(p.state)
                   from rusadm.ci_prem     p,
                        rusadm.ci_acct     a,
                        leskdata.tmp_filtr f
                  where f.acct_id = a.acct_id
                    and a.mailing_prem_id = p.prem_id)
    and &use_filter = '1'
     or nvl(&use_filter, '0') != '1');