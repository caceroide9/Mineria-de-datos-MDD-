import os
from tkinter import E 
from google.cloud import storage
from numpy import append
import xmltodict 
import json 
import ast
import mysql.connector
import datetime

connection = mysql.connector.connect(host='',
                                             database='',
                                             user='',
                                             password='')

os.environ['GOOGLE_APPLICATION_CREDENTIALS']='cuentadeservicio.json'


def list_blobs_with_prefix_to_mysql(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
    for blob in blobs: 
        a= blob.download_as_string()
        if((a)!=b''):
            r=blob.name
            s = a.decode('UTF-8')
            d = json.loads(s)
            print(r)
            print("Inicio Clase")
          
            r = Factura(d)
            r = Detalles(d)
            #r = Cabecera_Entera(d)

        else:
            r=blob.name
            print("Revisar",r)
        #
        #move_blobjson("documentos-tributarios-emitidos-en-json", r, "documentos-tributarios-emitidos-en-json-leidos", r)
        print("\n")
        
#self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
class Factura():
            def __init__(self,d):
                ############################ 33 Factura Electrónica ######################################
                if("Documento" in d['Document']['Content']['DTE']):
                    if(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                    
                        

                    
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                    

                      
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                    

                   
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                    

                    
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                    
                    
                     
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                    


                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                    

                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()



                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()
                       

                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        ##Nuevos Agregados en Cabecera
                        if('BcoPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['BcoPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago=None
                        
                        if('FchCancel' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchCancel']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel=None

                        if('FmaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FmaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago=None
                        
                        if('IndServicio' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['IndServicio']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio=None

                        if('MedioPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MedioPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago=None

                        if('MntBruto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['MntBruto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto=None

                        if('NumCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['NumCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago=None

                        if('PeriodoDesde' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoDesde']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde=None

                        if('SaldoInsol' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['SaldoInsol']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol=None

                        if('PeriodoHasta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['PeriodoHasta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta=None

                        if('TermPagoCdg' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoCdg']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg=None

                        if('TermPagoDias' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoDias']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias=None

                        if('TermPagoGlosa' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TermPagoGlosa']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa=None

                        if('TipoDespacho' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDespacho']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho=None

                        if('TpoCtaPago' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoCtaPago']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago=None

                        if('TpoImpresion' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoImpresion']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion=None
                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        
                        if('TpoTranCompra' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranCompra']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra=None

                        if('TpoTranVenta' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TpoTranVenta']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta=None
                        
                        #Emisor
                        if('CorreoEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CorreoEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor=None

                        if('CdgVendedor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgVendedor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor=None

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur=None

                        if('IdAdicEmisor' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['IdAdicEmisor']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor=None

                        if('RUTMandante' in d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']):
                                self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTMandante']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante=None

                        #Receptor
                        if('Recep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Recep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep=None

                        if('Contacto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Contacto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto=None

                        if('Fono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['Fono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono=None
                        
                        if('RUTSolicita' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTSolicita']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita=None

                        if('DirPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal=None

                        if('CmnaPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal=None

                        if('CiudadPostal' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                                self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadPostal']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal=None

                        #Monto
                        if('SaldoAnterior' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['SaldoAnterior']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior=0

                        if('VlrPagar' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['VlrPagar']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar=0

                        if('Prop' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Prop']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Prop=0

                        if('Terc' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['Terc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_Terc=0

                        if('MontoNF' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MontoNF']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF=0
                        

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Documento']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Documento']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Documento']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0

                        if('MntExe' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        if('TasaIVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0

                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Documento']['Encabezado']['Totales']['MntTotal']
                        

                       
        

                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        _BcoPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_BcoPago
                        _FchCancel=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchCancel
                        _FmaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FmaPago
                        _IndServicio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndServicio
                        _MedioPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MedioPago
                        _MntBruto=self.Document_Content_DTE_Documento_Encabezado_IdDoc_MntBruto
                        _NumCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_NumCtaPago
                        _PeriodoDesde=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoDesde
                        _SaldoInsol=self.Document_Content_DTE_Documento_Encabezado_IdDoc_SaldoInsol
                        _PeriodoHasta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_PeriodoHasta
                        _TermPagoCdg=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoCdg
                        _TermPagoDias=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoDias
                        _TermPagoGlosa=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TermPagoGlosa
                        _TipoDespacho=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDespacho
                        _TpoCtaPago=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoCtaPago
                        _TpoImpresion=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoImpresion
                        _TpoTranCompra=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranCompra
                        
                        _TpoTranVenta=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TpoTranVenta
                        _CorreoEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CorreoEmisor
                        _CdgVendedor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgVendedor
                        _CdgSIISucur=self.Document_Content_DTE_Documento_Encabezado_IdDoc_CdgSIISucur
                        _IdAdicEmisor=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IdAdicEmisor
                        _RUTMandante=self.Document_Content_DTE_Documento_Encabezado_IdDoc_RUTMandante
                        _Recep=self.Document_Content_DTE_Documento_Encabezado_Receptor_Recep
                        _Contacto=self.Document_Content_DTE_Documento_Encabezado_Receptor_Contacto
                        _Fono=self.Document_Content_DTE_Documento_Encabezado_Receptor_Fono
                        _RUTSolicita=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTSolicita
                        _DirPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirPostal
                        _CmnaPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaPostal
                        _CiudadPostal=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadPostal
                        _Totales_SaldoAnterior=self.Document_Content_DTE_Documento_Encabezado_Totales_SaldoAnterior
                        _VlrPagar=self.Document_Content_DTE_Documento_Encabezado_Totales_VlrPagar
                        _Prop=self.Document_Content_DTE_Documento_Encabezado_Totales_Prop
                        _Terc=self.Document_Content_DTE_Documento_Encabezado_Totales_Terc
                        _MontoNF=self.Document_Content_DTE_Documento_Encabezado_Totales_MontoNF


                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera ( pk_cabecera, TipoDte, Folio, FchEmis, IndTraslado, IndNoRebaja, BcoPago, FchCancel, FechVenc, FmaPago, IndServicio, MedioPago, MntBrutol, NumCtaPago, PeriodoDesde, PeriodoHasta, SaldInsol, TernPagoCdg, TermPagoDias, TermPagoGlosa, TipoDespacho, TpoCtaPago, TpoImpresion, TpoTranCompra, TpoTranVenta, RUTEmisor, CorreoEmisor, CdgVendedor, CdgSIISucur, IdAdicEmisor, RUTMandante, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, Recep, Contacto, Fono, RUTSolicita, DirPostal, CmnaPostal, CiudadPostal, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal, SaldoAnterior, VlrPagar, Prop, Terc, MontoNF) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_BcoPago),(_FchCancel),(_FechVenc),(_FmaPago),(_IndServicio),(_MedioPago),(_MntBruto),(_NumCtaPago),(_PeriodoDesde),(_PeriodoHasta),(_SaldoInsol),(_TermPagoCdg),(_TermPagoDias),(_TermPagoGlosa),(_TipoDespacho),(_TpoCtaPago),(_TpoImpresion),(_TpoTranCompra),(_TpoTranVenta),_RUTEmisor,(_CorreoEmisor),(_CdgVendedor),(_CdgSIISucur),(_IdAdicEmisor),(_RUTMandante),_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_Recep,_Contacto,_Fono,_RUTSolicita,_DirPostal,_CmnaPostal,_CiudadPostal,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal),int(_Totales_SaldoAnterior),int(_VlrPagar),int(_Prop),int(_Terc),int(_MontoNF))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                    else:
                        print("Revisar")
                
                elif("Liquidacion" in d['Document']['Content']['DTE']):
                    if(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['FchEmis']
                        
                        if('IndTraslado' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndNoRebaja' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        if('FchVenc' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None

                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['GiroEmis']
                        
                        if('Telefono' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']):
                            if(type(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['Telefono'])==list):
                                lm=''
                                for ele in (d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['Telefono']):
                                    lm=lm+ele+' '
                                    self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=lm
                            else:
                                self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['Acteco']

                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None

                        if('DirOrigen' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None
                        
                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'

                        if('CmnaRecep' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None

                        if('CiudadRecep' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None

                        if('MntNeto' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0
                        

                        if('MntExe' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        
                        if('TasaIVA' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0

                        if('IVA' in d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0
                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Totales']['MntTotal']
                        
    
                        _Content_all=self.Document_Content_all
                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal

                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera (pk_cabecera, TipoDTE, Folio, FchEmis, IndTraslado, IndNoRebaja,FechVenc, RUTEmisor, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente, RUTTrans, RUTChofer, NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_FechVenc),_RUTEmisor,_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest,_Patente,_RUTTrans,_RUTChofer,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                        
                    
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        pass

                      
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        pass

                   
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        pass

                    
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        pass
                    
                     
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        pass


                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        pass

                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        pass


                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        pass

                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        pass

                    else:
                        print("Revisar")
                    


                else:
                    if(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        result = json.dumps(d['Document']['Content']['DTE']['Encabezado']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk=(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['FchEmis'])
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['Folio']
                        self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['FchEmis']
                        if('IndTraslado' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['IndTraslado']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado=None
                        if('IndTraslado' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['IndNoRebaja']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja=None
                        
                        if('FchVenc' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']):
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['FchVenc']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc=None
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['RUTEmisor']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['RznSoc']
                        self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['GiroEmis']
                        if('Telefono' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['Telefono']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono=None

                        if(type(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['Acteco'])==list):
                            lm=''
                            for ele in (d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['Acteco']):
                                lm=lm+ele+' '
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=lm
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['Acteco']


                        if('CdgSIISucur' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['CdgSIISucur']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal=None
                        
                        if('DirOrigen' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['DirOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen=None

                        if('CmnaOrigen' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['CmnaOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen=None

                            
                        
                        if('CiudadOrigen' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']):
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Emisor']['CiudadOrigen']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen=None

                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['RUTRecep']
                        if('CdgIntRecep' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['CdgIntRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=None
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['CdgIntRecep']
                        self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['RznSocRecep']
                        
                        if('GiroRecep' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['GiroRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep=None

                        if('CorreoRecep' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['CorreoRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep=None
                        
                        if('DirRecep' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['DirRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep='-'


                        if('CmnaRecep' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['CmnaRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep=None
                        
                        if('CiudadRecep' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']):
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Receptor']['CiudadRecep']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep=None
                        
                        if('Transporte' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']):
                            if(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']==None):
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                                self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                            
                            else:
                                if('DirDest' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']['DirDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None

                            
                                if('CmnaDest' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']['CmnaDest']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                                    
                                if('Patente' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']['Patente']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None

                                if('RUTTrans' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']['RUTTrans']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                                
                                if('RUTChofer' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']['RUTChofer']
                                
                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None

                                if('NombreChofer' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']):
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Transporte']['NombreChofer']

                                else:
                                    self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None
                               
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer=None
                            self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer=None


                        if('MntNeto' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales']['MntNeto']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto=0
                        
                        if('MntExe' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales'] ):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales']['MntExe']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe=0
                        
                        if('TasaIVA' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales']):
                            self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales']['TasaIVA']
                        else:
                             self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA=0
                        
                        if('IVA' in d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales']):
                                self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales']['IVA']
                        else:
                            self.Document_Content_DTE_Documento_Encabezado_Totales_IVA=0
                        self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal=d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['Totales']['MntTotal']
                        
                        
                        _Content_all=self.Document_Content_all
                        _pk_cabecera= self.Document_Content_DTE_Documento_Encabezado_IdDoc_pk
                        _TipoDTE=self.Document_Content_DTE_Documento_Encabezado_IdDoc_TipoDTE
                        _Folio=self.Document_Content_DTE_Documento_Encabezado_IdDoc_Folio
                        _FchEmis=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchEmis
                        _IndTraslado=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndTraslado
                        _IndNoRebaja=self.Document_Content_DTE_Documento_Encabezado_IdDoc_IndNoRebaja
                        _FechVenc=self.Document_Content_DTE_Documento_Encabezado_IdDoc_FchVenc
                        _RUTEmisor=self.Document_Content_DTE_Documento_Encabezado_Emisor_RUTEmisor
                        _RznSoc=self.Document_Content_DTE_Documento_Encabezado_Emisor_RznSoc
                        _GiroEmis=self.Document_Content_DTE_Documento_Encabezado_Emisor_GiroEmis
                        _Telefono=self.Document_Content_DTE_Documento_Encabezado_Emisor_Telefono
                        _Acteco=self.Document_Content_DTE_Documento_Encabezado_Emisor_Acteco
                        _Sucursal=self.Document_Content_DTE_Documento_Encabezado_Emisor_Sucursal
                        _DirOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_DirOrigen
                        _CmnaOrigen=self.Document_Content_DTE_Documento_Encabezado_Emisor_CmnaOrigen
                        _CiudadOrigen= self.Document_Content_DTE_Documento_Encabezado_Emisor_CiudadOrigen
                        _RUTRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RUTRecep
                        _CdgIntRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CdgIntRecep
                        _RznSocRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_RznSocRecep
                        _GiroRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_GiroRecep
                        _CorreoRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CorreoRecep
                        _DirRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_DirRecep
                        _CmnaRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CmnaRecep
                        _CiudadRecep=self.Document_Content_DTE_Documento_Encabezado_Receptor_CiudadRecep
                        _DirDest= self.Document_Content_DTE_Documento_Encabezado_Transporte_DirDest
                        _CmnaDest=self.Document_Content_DTE_Documento_Encabezado_Transporte_CmnaDest
                        _Patente=self.Document_Content_DTE_Documento_Encabezado_Transporte_Patente
                        _RUTTrans=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTTrans
                        _RUTChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_RUTChofer
                        _NombreChofer=self.Document_Content_DTE_Documento_Encabezado_Transporte_NombreChofer
                        _MntNeto=self.Document_Content_DTE_Documento_Encabezado_Totales_MntNeto
                        _MntExe=self.Document_Content_DTE_Documento_Encabezado_Totales_MntExe
                        _TasaIVA=self.Document_Content_DTE_Documento_Encabezado_Totales_TasaIVA
                        _IVAm=self.Document_Content_DTE_Documento_Encabezado_Totales_IVA
                        _MntTotal= self.Document_Content_DTE_Documento_Encabezado_Totales_MntTotal
                        cursor = connection.cursor()
                        mySql_insert_query = """INSERT INTO Cabecera (pk_cabecera, TipoDTE, Folio, FchEmis, IndTraslado, IndNoRebaja,FechVenc, RUTEmisor, RznSoc, GiroEmis, Telefono, Acteco, Sucursal, DirOrigen, CmnaOrigen, CiudadOrigen, RUTRecep, CdgIntRecep, RznSocRecep, GiroRecep, CorreoRecep, DirRecep, CmnaRecep, CiudadRecep, DirDest, CmnaDest, Patente,RUTTrans,RUTTrans,NombreChofer, MntNeto, MntExe, TasaIVA, IVAm, MntTotal) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                        record = (_pk_cabecera,int(_TipoDTE),int(_Folio),(_FchEmis),(_IndTraslado),(_IndNoRebaja),(_FechVenc),_RUTEmisor,_RznSoc,_GiroEmis,_Telefono,(_Acteco),_Sucursal,_DirOrigen,_CmnaOrigen,_CiudadOrigen,_RUTRecep,_CdgIntRecep,_RznSocRecep,_GiroRecep,_CorreoRecep,_DirRecep,_CmnaRecep,_CiudadRecep,_DirDest,_CmnaDest, _Patente,_RUTTrans, _RUTTrans,_NombreChofer,int(_MntNeto),int(_MntExe),float(_TasaIVA),int(_IVAm),int(_MntTotal))
                        cursor.execute(mySql_insert_query, record)
                        connection.commit()

                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
                    
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        pass
                        
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        pass
  
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        pass
    
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        pass
     
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        pass
  
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        pass

                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        pass

                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        pass

                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        pass

                    else:
                        print("Revisar")


class Detalles():
            def __init__(self,d):
                if("Documento" in d['Document']['Content']['DTE']):
                                                ###### 33 #####
                    if(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        self.pk_detals=[]
                        self.listDetailsNroLinDet=[]
                        self.listDetails_CodItem_TpoCodigo=[]
                        self.listDetails_CodItem_VlrCodigo=[]
                        self.listDetailsNmbItem=[]
                        self.listDetailsDscItem=[]
                        self.listDetailsUnmdItem=[]
                        self.listQtyItem=[]
                        self.listPrcItem=[]
                        self.listMontoItem=[]
                        self.listCodImpAdic=[]
                        if(type(d['Document']['Content']['DTE']['Documento']['Detalle'])==list):
                            for x in range (len(d['Document']['Content']['DTE']['Documento']['Detalle'])):
                                self.pk_detals.append((d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['NroLinDet'])+'_'+d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                                self.listDetailsNroLinDet.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['NroLinDet'])
                                if('CdgItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    if('TpoCodigo' in d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CdgItem']):
                                        self.listDetails_CodItem_TpoCodigo.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CdgItem']['TpoCodigo'])
                                    else:
                                        self.listDetails_CodItem_TpoCodigo.append("-")
                                    if('VlrCodigo' in d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CdgItem']):
                                        self.listDetails_CodItem_VlrCodigo.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CdgItem']['VlrCodigo'])
                                    else:
                                        self.listDetails_CodItem_VlrCodigo.append("-")

                                else:
                                    self.listDetails_CodItem_TpoCodigo.append("-")
                                    self.listDetails_CodItem_VlrCodigo.append("-")


                                self.listDetailsNmbItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['NmbItem'])
                                if('DscItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listDetailsDscItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['DscItem'])
                                else:
                                    self.listDetailsDscItem.append("-")

                                if('QtyItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listQtyItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['QtyItem'])
                
                                else:
                                    self.listQtyItem.append("-")

                                if('UnmdItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listDetailsUnmdItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['UnmdItem'])
                                else:
                                    self.listDetailsUnmdItem.append("-")

                                if('PrcItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listPrcItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['PrcItem'])
                                else:
                                    self.listPrcItem.append(0)

                                self.listMontoItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['MontoItem'])
                                if ('CodImpAdic' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listCodImpAdic.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['listCodImpAdic'])
                                else:
                                    self.listCodImpAdic.append(0)

                        else:
                            if(type((d['Document']['Content']['DTE']['Documento']['Detalle']))!=list):
                                self.pk_detals.append((d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Detalle']['NroLinDet']))
                                self.listDetailsNroLinDet.append(d['Document']['Content']['DTE']['Documento']['Detalle']['NroLinDet'])
                                if('CdgItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    if('TpoCodigo' in d['Document']['Content']['DTE']['Documento']['Detalle']['CdgItem']):
                                        self.listDetails_CodItem_TpoCodigo.append(d['Document']['Content']['DTE']['Documento']['Detalle']['CdgItem']['TpoCodigo'])
                                    else:
                                        self.listDetails_CodItem_TpoCodigo.append("-")
                                    if('VlrCodigo' in d['Document']['Content']['DTE']['Documento']['Detalle']['CdgItem']):
                                        self.listDetails_CodItem_VlrCodigo.append(d['Document']['Content']['DTE']['Documento']['Detalle']['CdgItem']['VlrCodigo'])
                                    else:
                                        self.listDetails_CodItem_VlrCodigo.append("-")

                                else:
                                    self.listDetails_CodItem_TpoCodigo.append("-")
                                    self.listDetails_CodItem_VlrCodigo.append("-")


                                self.listDetailsNmbItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['NmbItem'])
                                if('DscItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listDetailsDscItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['DscItem'])
                                else:
                                    self.listDetailsDscItem.append("-")

                                if('QtyItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listQtyItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['QtyItem'])
                
                                else:
                                    self.listQtyItem.append("-")

                                if('UnmdItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listDetailsUnmdItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['UnmdItem'])
                                else:
                                    self.listDetailsUnmdItem.append("-")

                                if('PrcItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listPrcItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['PrcItem'])
                                else:
                                    self.listPrcItem.append(0)

                                self.listMontoItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['MontoItem'])
                                if ('CodImpAdic' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listCodImpAdic.append(d['Document']['Content']['DTE']['Documento']['Detalle']['CodImpAdic'])
                                else:
                                    self.listCodImpAdic.append(0)
                            
                            else:
                                print("Revisar")

                        for x,a,a1,a2,b,f,g,h,i,j,k in zip(self.pk_detals,self.listDetailsNroLinDet,self.listDetails_CodItem_TpoCodigo, self.listDetails_CodItem_VlrCodigo,self.listDetailsNmbItem,self.listDetailsDscItem,self.listDetailsUnmdItem,self.listQtyItem,self.listPrcItem,self.listMontoItem,self.listCodImpAdic):
                            _pk_detalles=x
                            _NroLinDet=a
                            _TpoCodigo=a1
                            _VlrCodigo=a2
                            _NmbItem=b
                            _DscItem=f
                            _QtyItem=g
                            _UnmdItem=h
                            _PrcItem=i
                            _MontoItem=j
                            _CodImpAdic=k
                            cursor = connection.cursor()
                            mySql_insert_query = """INSERT INTO Detalles (pk_detalles,NroLinDet,TpoCodigo,VlrCodigo,NmbItem,DscItem,QtyItem,UnmdItem,PrcItem,MontoItem,CodImpAdic) 
                                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                            registry = (_pk_detalles,_NroLinDet,_TpoCodigo,_VlrCodigo,_NmbItem,_DscItem,_QtyItem,_UnmdItem,_PrcItem,_MontoItem,_CodImpAdic)
                            cursor.execute(mySql_insert_query, registry,)
                            connection.commit()

                    
                ############################ 34 Factura No Afecta o Exenta Electrónica ##################      
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        pass

                ############################ 43 : Liquidación-Factura Electrónica ####################      
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        pass

                ############################ 46 Factura de Compra Electrónica ##############################      
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        pass

                ############################ 52 Guía de Despacho Electrónica ##############################      
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        pass
                    
                ############################ 56 Nota de Débito Electrónica ##################################      
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        pass
                    
                ########################## 61 Nota de Crédito Electrónica ##################################      
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        pass

                ########################## 110 Factura de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        pass

                ########################## 111 Nota de Débito de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        pass

                ########################## 112 Nota de Débito de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        pass

                    else:
                        print("Revisar") 
                
                elif("Liquidacion" in d['Document']['Content']['DTE']):
                    if(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        self.pk_detals=[]
                        self.listDetailsNroLinDet=[]
                        self.listDetails_CodItem_TpoCodigo=[]
                        self.listDetails_CodItem_VlrCodigo=[]
                        self.listDetailsNmbItem=[]
                        self.listDetailsDscItem=[]
                        self.listDetailsUnmdItem=[]
                        self.listQtyItem=[]
                        self.listPrcItem=[]
                        self.listMontoItem=[]
                        self.listCodImpAdic=[]
                        if(type(d['Document']['Content']['DTE']['Liquidacion']['Detalle'])==list):
                            for x in range (len(d['Document']['Content']['DTE']['Liquidacion']['Detalle'])):
                                self.pk_detals.append((d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['NroLinDet'])+'_'+d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['FchEmis'])
                                self.listDetailsNroLinDet.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['NroLinDet'])
                                if('CdgItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    if('TpoCodigo' in d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['CdgItem']):
                                        self.listDetails_CodItem_TpoCodigo.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['CdgItem']['TpoCodigo'])
                                    else:
                                        self.listDetails_CodItem_TpoCodigo.append("-")
                                    if('VlrCodigo' in d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['CdgItem']):
                                        self.listDetails_CodItem_VlrCodigo.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['CdgItem']['VlrCodigo'])
                                    else:
                                        self.listDetails_CodItem_VlrCodigo.append("-")

                                else:
                                    self.listDetails_CodItem_TpoCodigo.append("-")
                                    self.listDetails_CodItem_VlrCodigo.append("-")


                                self.listDetailsNmbItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['NmbItem'])
                                if('DscItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listDetailsDscItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['DscItem'])
                                else:
                                    self.listDetailsDscItem.append("-")

                                if('QtyItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listQtyItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['QtyItem'])
                
                                else:
                                    self.listQtyItem.append("-")

                                if('UnmdItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listDetailsUnmdItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['UnmdItem'])
                                else:
                                    self.listDetailsUnmdItem.append("-")

                                if('PrcItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listPrcItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['PrcItem'])
                                else:
                                    self.listPrcItem.append(0)

                                self.listMontoItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['MontoItem'])
                                if ('CodImpAdic' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listCodImpAdic.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle'][x]['listCodImpAdic'])
                                else:
                                    self.listCodImpAdic.append(0)

                        else:
                            if(type((d['Document']['Content']['DTE']['Liquidacion']['Detalle']))!=list):
                                self.pk_detals.append((d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Detalle']['NroLinDet'])+'_'+d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['FchEmis'])
                                self.listDetailsNroLinDet.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['NroLinDet'])
                                if('CdgItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    if('TpoCodigo' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']['CdgItem']):
                                        self.listDetails_CodItem_TpoCodigo.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['CdgItem']['TpoCodigo'])
                                    else:
                                        self.listDetails_CodItem_TpoCodigo.append("-")
                                    if('VlrCodigo' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']['CdgItem']):
                                        self.listDetails_CodItem_VlrCodigo.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['CdgItem']['VlrCodigo'])
                                    else:
                                        self.listDetails_CodItem_VlrCodigo.append("-")

                                else:
                                    self.listDetails_CodItem_TpoCodigo.append("-")
                                    self.listDetails_CodItem_VlrCodigo.append("-")


                                self.listDetailsNmbItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['NmbItem'])
                                if('DscItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listDetailsDscItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['DscItem'])
                                else:
                                    self.listDetailsDscItem.append("-")

                                if('QtyItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listQtyItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['QtyItem'])
                
                                else:
                                    self.listQtyItem.append("-")

                                if('UnmdItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listDetailsUnmdItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['UnmdItem'])
                                else:
                                    self.listDetailsUnmdItem.append("-")

                                if('PrcItem' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listPrcItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['PrcItem'])
                                else:
                                    self.listPrcItem.append(0)

                                self.listMontoItem.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['MontoItem'])
                                if ('CodImpAdic' in d['Document']['Content']['DTE']['Liquidacion']['Detalle']):
                                    self.listCodImpAdic.append(d['Document']['Content']['DTE']['Liquidacion']['Detalle']['CodImpAdic'])
                                else:
                                    self.listCodImpAdic.append(0)
                            
                            else:
                                print("Revisar")

                        for x,a,a1,a2,b,f,g,h,i,j,k in zip(self.pk_detals,self.listDetailsNroLinDet,self.listDetails_CodItem_TpoCodigo, self.listDetails_CodItem_VlrCodigo,self.listDetailsNmbItem,self.listDetailsDscItem,self.listDetailsUnmdItem,self.listQtyItem,self.listPrcItem,self.listMontoItem,self.listCodImpAdic):
                            _pk_detalles=x
                            _NroLinDet=a
                            _TpoCodigo=a1
                            _VlrCodigo=a2
                            _NmbItem=b
                            _DscItem=f
                            _QtyItem=g
                            _UnmdItem=h
                            _PrcItem=i
                            _MontoItem=j
                            _CodImpAdic=k
                            cursor = connection.cursor()
                            mySql_insert_query = """INSERT INTO Detalles (pk_detalles,NroLinDet,TpoCodigo,VlrCodigo,NmbItem,DscItem,QtyItem,UnmdItem,PrcItem,MontoItem,CodImpAdic) 
                                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                            registry = (_pk_detalles,_NroLinDet,_TpoCodigo,_VlrCodigo,_NmbItem,_DscItem,_QtyItem,_UnmdItem,_PrcItem,_MontoItem,_CodImpAdic)
                            cursor.execute(mySql_insert_query, registry,)
                            connection.commit()

                    
                ############################ 34 Factura No Afecta o Exenta Electrónica ##################      
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        pass

                ############################ 43 : Liquidación-Factura Electrónica ####################      
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        pass

                ############################ 46 Factura de Compra Electrónica ##############################      
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        pass

                ############################ 52 Guía de Despacho Electrónica ##############################      
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        pass
                    
                ############################ 56 Nota de Débito Electrónica ##################################      
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        pass
                    
                ########################## 61 Nota de Crédito Electrónica ##################################      
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        pass

                ########################## 110 Factura de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        pass

                ########################## 111 Nota de Débito de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        pass

                ########################## 112 Nota de Débito de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        pass

                    else:
                        print("Revisar")

                else:
                    ############################ 33  ####################    
                    if(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        self.pk_detals=[]
                        self.listDetailsNroLinDet=[]
                        self.listDetails_CodItem_TpoCodigo=[]
                        self.listDetails_CodItem_VlrCodigo=[]
                        self.listDetailsNmbItem=[]
                        self.listDetailsDscItem=[]
                        self.listDetailsUnmdItem=[]
                        self.listQtyItem=[]
                        self.listPrcItem=[]
                        self.listMontoItem=[]
                        self.listCodImpAdic=[]
                        if(type(d['Document']['Content']['DTE']['Documento']['Detalle'])==list):
                            for x in range (len(d['Document']['Content']['DTE']['Documento']['Detalle'])):
                                self.pk_detals.append((d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['NroLinDet']))
                                self.listDetailsNroLinDet.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['NroLinDet'])
                                if('CdgItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    if('TpoCodigo' in d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CdgItem']):
                                        self.listDetails_CodItem_TpoCodigo.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CdgItem']['TpoCodigo'])
                                    else:
                                        self.listDetails_CodItem_TpoCodigo.append("-")
                                    if('VlrCodigo' in d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CdgItem']):
                                        self.listDetails_CodItem_VlrCodigo.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CdgItem']['VlrCodigo'])
                                    else:
                                        self.listDetails_CodItem_VlrCodigo.append("-")

                                else:
                                    self.listDetails_CodItem_TpoCodigo.append("-")
                                    self.listDetails_CodItem_VlrCodigo.append("-")


                                self.listDetailsNmbItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['NmbItem'])
                                if('DscItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listDetailsDscItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['DscItem'])
                                else:
                                    self.listDetailsDscItem.append("-")

                                if('QtyItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listQtyItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['QtyItem'])
                
                                else:
                                    self.listQtyItem.append("-")

                                if('UnmdItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listDetailsUnmdItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['UnmdItem'])
                                else:
                                    self.listDetailsUnmdItem.append("-")

                                if('PrcItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listPrcItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['PrcItem'])
                                else:
                                    self.listPrcItem.append(0)

                                self.listMontoItem.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['MontoItem'])
                                if ('CodImpAdic' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listCodImpAdic.append(d['Document']['Content']['DTE']['Documento']['Detalle'][x]['CodImpAdic'])
                                else:
                                    self.listCodImpAdic.append(0)

                        else:
                            if(type((d['Document']['Content']['DTE']['Documento']['Detalle']))!=list):
                                self.pk_detals.append((d['Document']['Content']['DTE']['Documento']['Encabezado']['Emisor']['RUTEmisor'].replace("-", ""))+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE'])+'_'+(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['Folio'])+'_'+(d['Document']['Content']['DTE']['Documento']['Detalle']['NroLinDet']))
                                self.listDetailsNroLinDet.append(d['Document']['Content']['DTE']['Documento']['Detalle']['NroLinDet'])
                                if('CdgItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    if('TpoCodigo' in d['Document']['Content']['DTE']['Documento']['Detalle']['CdgItem']):
                                        self.listDetails_CodItem_TpoCodigo.append(d['Document']['Content']['DTE']['Documento']['Detalle']['CdgItem']['TpoCodigo'])
                                    else:
                                        self.listDetails_CodItem_TpoCodigo.append("-")
                                    if('VlrCodigo' in d['Document']['Content']['DTE']['Documento']['Detalle']['CdgItem']):
                                        self.listDetails_CodItem_VlrCodigo.append(d['Document']['Content']['DTE']['Documento']['Detalle']['CdgItem']['VlrCodigo'])
                                    else:
                                        self.listDetails_CodItem_VlrCodigo.append("-")

                                else:
                                    self.listDetails_CodItem_TpoCodigo.append("-")
                                    self.listDetails_CodItem_VlrCodigo.append("-")


                                self.listDetailsNmbItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['NmbItem'])
                                if('DscItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listDetailsDscItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['DscItem'])
                                else:
                                    self.listDetailsDscItem.append("-")

                                if('QtyItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listQtyItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['QtyItem'])
                
                                else:
                                    self.listQtyItem.append("-")

                                if('UnmdItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listDetailsUnmdItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['UnmdItem'])
                                else:
                                    self.listDetailsUnmdItem.append("-")

                                if('PrcItem' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listPrcItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['PrcItem'])
                                else:
                                    self.listPrcItem.append(0)

                                self.listMontoItem.append(d['Document']['Content']['DTE']['Documento']['Detalle']['MontoItem'])
                                if ('CodImpAdic' in d['Document']['Content']['DTE']['Documento']['Detalle']):
                                    self.listCodImpAdic.append(d['Document']['Content']['DTE']['Documento']['Detalle']['listCodImpAdic'])
                                else:
                                    self.listCodImpAdic.append(0)
                            
                            else:
                                print("Revisar")

                        for x,a,a1,a2,b,f,g,h,i,j,k in zip(self.pk_detals,self.listDetailsNroLinDet,self.listDetails_CodItem_TpoCodigo, self.listDetails_CodItem_VlrCodigo,self.listDetailsNmbItem,self.listDetailsDscItem,self.listDetailsUnmdItem,self.listQtyItem,self.listPrcItem,self.listMontoItem,self.listCodImpAdic):
                            _pk_detalles=x
                            _NroLinDet=a
                            _TpoCodigo=a1
                            _VlrCodigo=a2
                            _NmbItem=b
                            _DscItem=f
                            _QtyItem=g
                            _UnmdItem=h
                            _PrcItem=i
                            _MontoItem=j
                            _CodImpAdic=k
                            cursor = connection.cursor()
                            mySql_insert_query = """INSERT INTO Detalles (pk_detalles,NroLinDet,TpoCodigo,VlrCodigo,NmbItem,DscItem,QtyItem,UnmdItem,PrcItem,MontoItem,CodImpAdic) 
                                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                            registry = (_pk_detalles,_NroLinDet,_TpoCodigo,_VlrCodigo,_NmbItem,_DscItem,_QtyItem,_UnmdItem,_PrcItem,_MontoItem,_CodImpAdic)
                            cursor.execute(mySql_insert_query, registry,)
                            connection.commit()

                        ############################ 34  ####################    
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        pass
                    
                ############################ 43 : Liquidación-Factura Electrónica ####################      
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        pass

                ############################ 46 Factura de Compra Electrónica ##############################      
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        pass

                ############################ 52 Guía de Despacho Electrónica ##############################      
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        pass


                ############################ 56 Nota de Débito Electrónica ##################################      
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        pass

                ########################## 61 Nota de Crédito Electrónica ##################################      
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        pass


                ########################## 110 Factura de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        pass


                ########################## 111 Nota de Débito de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        pass


                ########################## 112 Nota de Débito de Exportación ##################################
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        pass

                    else:
                        print("Revisar")
                

class Cabecera_Entera():
            def __init__(self,d):
                if("Documento" in d['Document']['Content']['DTE']):
                    if(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all

                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
                        
                        

                    
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all

                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                      
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                   
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
                    
                     
                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()


                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()


                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    elif(d['Document']['Content']['DTE']['Documento']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        result = json.dumps(d['Document']['Content']['DTE']['Documento']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    else:
                        print("Revisar")
                
                elif("Liquidacion" in d['Document']['Content']['DTE']):
                    if(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
                    
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                      
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                   
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
                    
                     
                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()


                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()


                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    elif(d['Document']['Content']['DTE']['Liquidacion']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        result = json.dumps(d['Document']['Content']['DTE']['Liquidacion']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    else:
                        print("Revisar")
                    


                else:
                    if(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='33'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
                    
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='34'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
                        
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='43'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
  
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='46'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
    
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='52'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
     
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='56'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()
  
                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='61'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='110'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='111'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    elif(d['Document']['Content']['DTE']['Exportaciones']['Encabezado']['IdDoc']['TipoDTE']=='112'):
                        result = json.dumps(d['Document']['Content']['DTE']['Exportaciones']['Encabezado'])
                        self.Document_Content_all=result
                        _Content_all=self.Document_Content_all
                        cursor = connection.cursor()
                        mySql_insert_query_2="""INSERT INTO Tags_Cabecera(cabecera)
                                    VALUES (%s)"""
                        datos=(_Content_all,)
                        cursor.execute(mySql_insert_query_2,datos)
                        connection.commit()

                    else:
                        print("Revisar")

"""
def move_blobjson(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_bucket.delete_blob(blob_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )

"""                 



list_blobs_with_prefix_to_mysql("documentos-tributarios-recibidos-historicos-formato-json",None)


