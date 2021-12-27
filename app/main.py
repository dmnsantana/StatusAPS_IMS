import warnings
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
import os

from utils import *


def CreateDataframe(status_aps, df_service_manager, df_bts, df_fases, num, aps_revisar):
    status_aps = status_aps.reset_index()
    filter1 = df_service_manager[df_service_manager.ID_BENEFICIARIO.isin(status_aps.ID_BENEFICIARIO)]
    filter1 = filter1[['ID_MINTIC', 'ID_BENEFICIARIO', "IM", 'ESTADO', 'PRIORIDAD', 'TÍTULO', 'FECHA_HORA_DE_APERTURA']]
    status_aps = pd.merge(filter1, status_aps, on='ID_BENEFICIARIO')

    filter2 = df_bts[df_bts.ID_MINTIC.isin(status_aps.ID_MINTIC)]
    filter3 = df_fases[df_fases.ID_MINTIC.isin(status_aps.ID_MINTIC)]

    fases_dda = filter3[["ID_MINTIC", "MUNICIPIO", "FASE_OFICIAL", "DDA"]]
    bts = filter2[['ID_MINTIC', 'BTS', 'REGIONAL_O&M']]

    id_bts = pd.merge(bts, status_aps, on='ID_MINTIC')

    Pag = pd.merge(id_bts, fases_dda, on='ID_MINTIC', how='outer')

    ## REVISAR CDs
    if num == 1:
        Pag = pd.merge(aps_revisar, Pag, on='ID_BENEFICIARIO')
        Pag = Pag.drop(columns=["online", "offline"], axis=1)

        Pag["DIAGNOSTICO"] = "Se realiza la validacion del CD para el ID_MINTIC BENEFICIARIO: " + Pag[
            "ID_BENEFICIARIO"].astype(str) + ", ubicado en el MUNICIPIO: " \
                             + Pag["MUNICIPIO"].astype(str) + ", con DDA: " + Pag["DDA"].astype(
            str) + " encontrando falla en el AP: " + Pag["DEVICE_NAME"].astype(
            str) + " por lo cual es necesario generar Tarea para desplazar personal a sitio y realizar las respectivas validaciones." \
                             + "Nota: Se solicita al personal que se va a desplazar a sitio llevar repuestos para las APS, Tarjeta de Red para la UPS,Cable UTP Categoria 6, Garantizar que el PC soporte troncalizacion de Vlan y funcione correctamente la tarjeta de RED."

        #Pag = Pag.groupby(['ID_MINTIC', 'ID_BENEFICIARIO', 'BTS', 'IM', 'ESTADO','REGIONAL_O&M',
         #                  'PRIORIDAD', "TÍTULO", "FECHA_HORA_DE_APERTURA", 'MUNICIPIO', 'FASE_OFICIAL', 'DDA',
          #                 "DIAGNOSTICO"])['DEVICE_NAME'].apply(list)

    ## REVISAR Tx
    elif num == 2:

        Pag["DIAGNOSTICO"] = "Se realiza la validacion de la Ruta de Tx,para el ID_MINTIC BENEFICIARIO: " + Pag[
            "ID_BENEFICIARIO"] + ", ubicado en el MUNICIPIO: " + Pag["MUNICIPIO"] + ", con DDA: " + Pag[
                                 "DDA"] + ", donde se descartan fallas  en el los distintos tramos de la Ruta de Tx" \
                             + "por lo cual es necesario generar Tarea para desplazar personal a sitio y realizar las respectivas validaciones." \
                             + "Nota: Se solicita al personal que se va a desplazar a sitio llevar repuestos para las APS, Tarjeta de Red para la UPS,Cable UTP Categoria 6, Garantizar que el PC soporte troncalizacion de Vlan y funcione correctamente la tarjeta de RED."

        #Pag = \
         #   Pag.groupby(
          #      ['ID_MINTIC', 'BTS', 'ID_BENEFICIARIO', 'IM', 'ESTADO', 'REGIONAL_O&M', 'PRIORIDAD', "TÍTULO", "FECHA_HORA_DE_APERTURA",
           #      'MUNICIPIO', 'FASE_OFICIAL', 'DIAGNOSTICO'])['DDA'].apply(list)

    ## APs ONLINE
    else:

        Pag["DIAGNOSTICO"] = "Se realiza el monitorieo para el ID_MINTIC BENEFICIARIO: " + Pag[
            "ID_BENEFICIARIO"] + ", ubicado en el MUNICIPIO: " + Pag["MUNICIPIO"] + ", con DDA: " + Pag[
                                 "DDA"] + ", donde se evidencia que el CD se encuentra operativo, y todos los APs se encuentran Online."

        #Pag = \
         #   Pag.groupby(
          #      ['ID_MINTIC', 'BTS', 'ID_BENEFICIARIO', 'IM', 'ESTADO', 'REGIONAL_O&M', 'PRIORIDAD', "TÍTULO", "FECHA_HORA_DE_APERTURA",
           #      'MUNICIPIO', 'FASE_OFICIAL', 'DIAGNOSTICO'])['DDA'].apply(list)

    return Pag


def to_excel_sheet(df_aps_1, df_centrodigital, df_rutaTx, df_allonline):
    print("Entro funcion excel")
    f = datetime.today().__str__()
    f = f.replace(":", "-")
    f = f.replace(".", "-")
    path = os.getcwd()
    print("ruta os", path)
    print(path + "\APS_IM_STATUS-" + f + ".xlsx")

    with pd.ExcelWriter(path + "\ExcelsGenerados\APS_IM_STATUS-" + f + ".xlsx") as writer:
        df_aps_1.to_excel(writer, sheet_name="APS_STATUS")
        df_centrodigital.to_excel(writer, sheet_name="REVISAR_CD")
        df_rutaTx.to_excel(writer, sheet_name="REVISAR_TX")
        df_allonline.to_excel(writer, sheet_name="CDS_ONLINE")

        print("Mensaje", "Excel creado con exito")


if __name__ == '__main__':
    # warnings.filterwarnings('ignore')

    # Lectura de datos
    df_bts = Bts()
    df_generator_script = GeneratorScript()
    df_service_manager = ServiceManager()
    # print(df_service_manager)
    df_fases = FaseDDA()
    # ---------ASIGNADOS------------------------------------------------------------------------------------------------
    # -------cnMaestro--------------------------------------------------------------------------------------------------
    df_cnmaestro = cnMaestroAPs()
    df_cnmaestro = cnMaestro(df_cnmaestro)
    # ------------------------------------------------------------------------------------------------------------------

    # ---Filtrar incidentes por id beneficiario y cruzar con cnMaestro
    df_indidentes_status = df_cnmaestro[df_cnmaestro.ID_BENEFICIARIO.isin(df_service_manager.ID_BENEFICIARIO)]
    df_aps = df_indidentes_status.groupby(['ID_BENEFICIARIO', "NETWORK", "DEVICE_NAME"])[
        'STATUS'].value_counts().unstack().fillna(0)
    df_aps_1 = df_aps
    df_aps = df_indidentes_status.groupby(['ID_BENEFICIARIO'])['STATUS'].value_counts().unstack().fillna(0)

    aps_on_off = df_aps.loc[(df_aps['online'] > 0.0)]
    aps_on_off = aps_on_off.loc[(aps_on_off['online'] < 3.0)]
    aps_revisar = aps_on_off.reset_index()
    aps_revisar = df_cnmaestro[df_cnmaestro.ID_BENEFICIARIO.isin(aps_revisar.ID_BENEFICIARIO)]
    aps_revisar = aps_revisar.groupby(['ID_BENEFICIARIO', "DEVICE_NAME"])['STATUS'].value_counts().unstack().fillna(0)
    aps_revisar = aps_revisar.reset_index()
    aps_revisar = aps_revisar.loc[aps_revisar['offline'] > 0.0]
    aps_revisar = aps_revisar.groupby(['ID_BENEFICIARIO'])['DEVICE_NAME'].apply(list)
    aps_offline = df_aps.loc[df_aps['online'] == 0.0]
    aps_online = df_aps.loc[df_aps['online'] == 3]

    # # Crear dataframe con todos los cruces
    df_centrodigital = CreateDataframe(aps_on_off, df_service_manager, df_bts, df_fases, 1, aps_revisar)
    df_rutaTx = CreateDataframe(aps_offline, df_service_manager, df_bts, df_fases, 2, aps_revisar)
    df_allonline = CreateDataframe(aps_online, df_service_manager, df_bts, df_fases, 3, aps_revisar)

    to_excel_sheet(df_aps_1, df_centrodigital, df_rutaTx, df_allonline)
