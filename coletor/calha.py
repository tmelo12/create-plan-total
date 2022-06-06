def nomeCalha(municipio):
    if(municipio == "GUAJARÁ"    or 
       municipio == "IPIXUNA"    or
       municipio == "EIRUNEPÉ"   or
       municipio == "ITAMARATI"  or
       municipio == "JURUÁ"      or
       municipio == "CARAUARI"   or
       municipio == "ENVIRA"     
      ):
        return "JURUÁ"
    
    elif(municipio == "BOCA DO ACRE" or
       municipio == "PAUINI"       or
       municipio == "LÁBREA"       or
       municipio == "CANUTAMA"     or
       municipio == "TAPAUÁ"       or
       municipio == "BERURI"    
      ):
        return "PURUS"
    
    elif(municipio == "HUMAITÁ"              or
       municipio == "APUÍ"                 or
       municipio == "MANICORÉ"             or
       municipio == "NOVO ARIPUANÃ"        or
       municipio == "BORBA"                or
       municipio == "NOVA OLINDA DO NORTE"
      ):
        return "MADEIRA"
    
    elif(municipio == "ATALAIA DO NORTE"      or
       municipio == "BENJAMIN CONSTANT"     or
       municipio == "TABATINGA"             or
       municipio == "SÃO PAULO DE OLIVENÇA" or
       municipio == "AMATURÁ"               or
       municipio == "SANTO ANTÔNIO DO IÇÁ"  or
       municipio == "TONANTINS"
      ):
        return "ALTO SOLIMÕES"
   
    elif(municipio == "JUTAÍ"     or
       municipio == "FONTE BOA" or
       municipio == "JAPURÁ"    or
       municipio == "MARAÃ"     or
       municipio == "UARINI"    or
       municipio == "ALVARÃES"  or
       municipio == "TEFÉ"      or
       municipio == "COARI"
          ):
            return "MÉDIO SOLIMÕES"
        
    elif(municipio == "CODAJÁS"          or
       municipio == "ANORI"            or
       municipio == "ANAMÃ"            or
       municipio == "CAAPIRANGA"       or
       municipio == "MANACAPURU"       or
       municipio == "IRANDUBA"         or
       municipio == "MANAQUIRI"        or
       municipio == "CAREIRO CASTANHO" or
       municipio == "CAREIRO DA VÁRZEA"
      ):
        return "BAIXO SOLIMÕES"
    
    elif(municipio == "ITACOATIARA"           or
       municipio == "PRESIDENTE FIGUEIREDO" or
       municipio == "RIO PRETO DA EVA"      or
       municipio == "SILVES"                or
       municipio == "AUTAZES"               or
       municipio == "URUCURITUBA"           or
       municipio == "ITAPIRANGA"           
      ):
        return "MÉDIO AMAZONAS"
    
    elif(municipio == "BARREIRINHA"             or
       municipio == "BOA VISTA DO RAMOS"      or
       municipio == "NHAMUNDÁ"                or
       municipio == "URUCARÁ"                 or
       municipio == "SÃO SEBASTIÃO DO UATUMÃ" or
       municipio == "PARINTINS"               or
       municipio == "MAUÉS"
      ):
        return "BAIXO AMAZONAS"
    
    elif(municipio == "SÃO GABRIEL DA CACHOEIRA"  or
       municipio == "SANTA ISABEL DO RIO NEGRO" or
       municipio == "BARCELOS"                  or
       municipio == "NOVO AIRÃO"                or
       municipio == "MANAUS" 
      ):
        return "RIO NEGRO"
    
    else:
        return "NOME CALHA"