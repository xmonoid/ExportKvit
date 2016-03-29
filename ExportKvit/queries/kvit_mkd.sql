
call lcmccb.pcm_kvee_mkd_id(&pdat, &pleskgesk, &pdb_lesk , &pnot_empty, &use_filter, &mkd_id);

select  bd_lesk,
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
        CountRes,
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
        OldPokaz5,
        Pokaz1,
        Pokaz2,
        Pokaz3,
        Pokaz4,
        Pokaz5,
        KoefTr1,
        KoefTr2,
        KoefTr3,
        KoefTr4,
        KoefTr5,
        Kwt1,
        Kwt2,
        Kwt3,
        Kwt4,
        Kwt5,
        Tarif1,
        Tarif2,
        Tarif3,
        Tarif4,
        Tarif5,
        Sum1,
        Sum2,
        Sum3,
        Sum4,
        Sum5,
        Sum,
        to_char(round(lcmccb.fcm_to_number(RKwt1), 2)) RKwt1,
        to_char(round(lcmccb.fcm_to_number(RKwt2), 2)) RKwt2,
        to_char(round(lcmccb.fcm_to_number(RKwt3), 2)) RKwt3,
        to_char(round(lcmccb.fcm_to_number(RKwt4), 2)) RKwt4,
        to_char(round(lcmccb.fcm_to_number(RKwt5), 2)) RKwt5,
        to_char(round(lcmccb.fcm_to_number(RKwt), 2)) RKwt,
        RSum1,
        RSum2,
        RSum3,
        RSum4,
        RSum5,
        Rsum,
        Total1,
        Total2,
        Total3,
        Total4,
        Total5,
        Total,
        DebtIn,
        TotalPay,
        PrePay,
        OdnTitle,
        Odn1,
        Odn2,
        Odn3,
        Odn4,
        Odn5,
        Odn6,
        Odn7,
        Odn8,
        DebtInTitle,
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
        SOdpu1,
        SOdpu2,
        SOdpu3,
        SOdpu4,
        SOdpu5,
        SOdpu6,
        SOdpu7,
        SOdpu8,
        SOdpu9,
        SOdpu10,
        EOdpu1,
        EOdpu2,
        EOdpu3,
        EOdpu4,
        EOdpu5,
        EOdpu6,
        EOdpu7,
        EOdpu8,
        EOdpu9,
        EOdpu10,
        Diff1,
        Diff2,
        Diff3,
        Diff4,
        Diff5,
        Diff6,
        Diff7,
        Diff8,
        Diff9,
        Diff10,
        Ratio1,
        Ratio2,
        Ratio3,
        Ratio4,
        Ratio5,
        Ratio6,
        Ratio7,
        Ratio8,
        Ratio9,
        Ratio10,
        TotalKwt1,
        TotalKwt2,
        TotalKwt3,
        TotalKwt4,
        TotalKwt5,
        TotalKwt6,
        TotalKwt7,
        TotalKwt8,
        TotalKwt9,
        TotalKwt10,
        TotalKwt,
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
   from (select *
           from lcmccb.CM_KVEE_MKD_CSV k 
          where pdat = &pdat
            and (&use_filter != '1'
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
                                                    and pr.prnt_prem_id = &mkd_id)))
          order by bd_lesk, 
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