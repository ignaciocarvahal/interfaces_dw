import sys
import psycopg2
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QCalendarWidget, QPushButton, QLabel
import pandas as pd
class DateRangePicker(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Selector de Rango de Fechas")
        self.setGeometry(100, 100, 400, 300)

        layout = QVBoxLayout()

        self.calendar_desde = QCalendarWidget()
        layout.addWidget(QLabel("Selecciona la fecha desde:"))
        layout.addWidget(self.calendar_desde)

        self.calendar_hasta = QCalendarWidget()
        layout.addWidget(QLabel("Selecciona la fecha hasta:"))
        layout.addWidget(self.calendar_hasta)

        self.btn_submit = QPushButton("Aceptar")
        self.btn_submit.clicked.connect(self.get_selected_dates)
        layout.addWidget(self.btn_submit)

        self.setLayout(layout)

    def get_selected_dates(self):
        fecha_desde = self.calendar_desde.selectedDate().toString("dd-MM-yyyy")
        fecha_hasta = self.calendar_hasta.selectedDate().toString("dd-MM-yyyy")

        # Datos de conexión
        host = "190.171.188.230"
        port = "5432"
        database = "topusDB"
        user = "user_solo_lectura"
        password = "4l13nW4r3.C0ntr4s3n4.S0l0.L3ctur4"
        
        try:
            # Conexión a la base de datos
            connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            cursor = connection.cursor()

            # Ejecución de la consulta SQL
            query = """
            SELECT 
                ser.id as fk_servicio,
                eta_1.id as fk_etapa,
                ser.estado,
                ser.referencia,
                CONCAT(TRIM(comer.usu_nombre), ' ', TRIM(comer.usu_apellido)) AS comercial_nombre,
                cli_fact.cli_nombre AS cli_fact_nombre,
                cli_desp.cli_codigo AS cli_desp_nombre,
                com_1.comuna_nombre as comuna_nombre,
                ser.fk_tipo_servicio as servicio_codigo,
                coalesce(nave.nave_nombre,'') as servicio_nave_nombre,
                coalesce(eta.eta_fecha,'') as eta_fecha,
                ser.fk_tipo_carga,
                ser.numero_contenedor,
                coalesce(cont_tip.cont_nombre,'') as cont_tipo_nombre,
                coalesce(cont_tam.conttam_tamano,'') as cont_tamano,
                ser.contenedor_peso,
                ser.contenedor_peso_carga,
                coalesce(eta_1.tipo, 0) as etapa_tipo,
                coalesce (eta_1.titulo, '') as etapa_titulo,
                coalesce(eta_1.fecha, '') as etapa_1_fecha,
                coalesce(eta_1.hora, '') as etapa_1_hora,
                eta_0.fk_direccion as direccion_id_salida,
                dir_1.id as direccion_id_llegada,
                (SELECT temp1.tiempo FROM public.tiempodistanciadirecciones as temp1 where eta_0.fk_direccion=temp1.dir1 and temp1.dir2=dir_1.id order by id desc limit 1) as tiempo_minutos,
                (SELECT temp1.distancia FROM public.tiempodistanciadirecciones as temp1 where eta_0.fk_direccion=temp1.dir1 and temp1.dir2=dir_1.id order by id desc limit 1) as distancia_mts,
                coalesce(dir_1.nombre,'') as etapa_1_lugar_nombre,
                concat(dir_1.nombre) as etapa_1_direccion_texto,
                concat(cond_1.usu_rut) as etapa_1_conductor_rut,
                concat ( TRIM(coalesce(cond_1.usu_nombre,'')),' ',TRIM(coalesce(cond_1.usu_apellido,'')) ) as etapa_1_conductor_nombre,
                coalesce(tract_1.patente,'') as etapa_1_tracto,
                comer.usu_nombre as nombre_comercial,
                coalesce(ser.almacenaje_principal,'') as almacenaje_principal,
                concat( coalesce(ser.cont_fila,''),'-',coalesce(ser.cont_columna,''),'-',coalesce(ser.cont_posicion) ) as posicion_ubicacion,
                case 
                    when ser.cont_tipo_mov='SALIDA' and ser.cont_tipo='VACIO' THEN concat('S-V ',coalesce(ser.cont_hora,''))
                    when ser.cont_tipo_mov='SALIDA' and ser.cont_tipo='LLENO' THEN concat('S-F ',coalesce(ser.cont_hora,''))
                    when ser.cont_tipo_mov='INGRESO' and ser.cont_tipo='VACIO' THEN 'A-V'
                    when ser.cont_tipo_mov='INGRESO' and ser.cont_tipo='LLENO' THEN 'A-F'
                    when ser.cont_tipo_mov='CAMBIO POSICION' and ser.cont_tipo='VACIO' THEN 'A-V'
                    when ser.cont_tipo_mov='CAMBIO POSICION' and ser.cont_tipo='LLENO' THEN 'A-F'
                    else '' 
                end as posicion_tipo,
                ser_logs."createdAt" as fecha_cierre
            FROM
                public.servicios as ser
                inner join public.usuarios as comer on ser.fk_comercial=comer.usu_rut
                left join public.clientes as cli_fact on ser.fk_cliente_facturacion=cli_fact.cli_codigo
                left join public.clientes as cli_desp on ser.fk_cliente_despacho=cli_desp.cli_codigo
                left join public.naves as nave on ser.fk_nave=nave.nave_id
                left join public.naves_etas as eta on ser.fk_eta=eta.eta_id
                left join public.contenedores_tipos as cont_tip on ser.fk_tipo_contenedor=cont_tip.cont_id
                left join public.contenedores_tamanos as cont_tam on ser.fk_contenedor_tamano=cont_tam.conttam_id
                
                left join public.servicios_etapas as eta_1 on ser.id=eta_1.fk_servicio
                left join public.direcciones as dir_1 on eta_1.fk_direccion=dir_1.id
                left join public.comunas as com_1 on dir_1."comunaComunaId"=com_1.comuna_id
                left join public.servicios_etapas_conductores as cond_eta_1 on eta_1.id=cond_eta_1.fk_etapa
                left join public.usuarios as cond_1 on cond_eta_1.fk_conductor=cond_1.usu_rut
                left join public.taller_equipos as tract_1 on cond_eta_1.fk_tracto=tract_1.id
                
                left join public.servicios_etapas as eta_0 on eta_1.fk_etapa_anterior=eta_0.id
                left join public.servicios_logs as ser_logs on ser.id = ser_logs.id
            WHERE
                
                 ser.fk_tipo_carga != 'LCL'
                AND eta_1.fecha BETWEEN %s AND %s
                AND ser_logs.accion = ' UPDATE ESTADO 2 A SERVICIO'
            ORDER BY
                ser.id ASC
        """
            cursor.execute(query, (fecha_desde, fecha_hasta))
            
            
            # Obtener los resultados
            rows = cursor.fetchall()      
            
            

       
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

            # Convertir todas las fechas a strings
            df = df.applymap(lambda x: x.strftime('%d-%m-%Y') if isinstance(x, pd.Timestamp) else x)
            print(df)
            print(df)
            
            
            # Guardar DataFrame como archivo Excel
            output_file = "resultados_servicios.xlsx"
            df.to_excel(output_file, index=False)
            print(f"Resultados guardados en '{output_file}'")
                        
            
            # Cerrar la conexión
            cursor.close()
            connection.close()
            
        except (Exception, psycopg2.Error) as error:
            print("Error al obtener datos de PostgreSQL:", error)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = DateRangePicker()
    window.show()
    sys.exit(app.exec_())
