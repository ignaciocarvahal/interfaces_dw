import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QVBoxLayout, QMessageBox
import sys


def subir_datos():
    try:
        # Leer los datos desde el archivo Excel
        excel_file = "mantenimiento.xlsx"
        df = pd.read_excel(excel_file)
        df = df.dropna(subset=['kilometros'])

        categoria = []
        kilometros = df['kilometros']

        for kilometraje in kilometros:
            if kilometraje == 0 or pd.isna(kilometraje):
                categoria.append("nada")
            elif kilometraje < -1000:
                categoria.append('Más de 1000 kilometros pasado')
            elif -1000 <= kilometraje < 1000:
                categoria.append('Pasado menos de 1000 kilometros')
            elif 1000 <= kilometraje < 2000:
                categoria.append('Quedan menos de 2000 kilometros para mantenimiento')
            else:
                categoria.append('Quedan más de 2000 kilometros para mantenimiento')

        df['categorias'] = categoria

        # Definir la conexión a la base de datos
        host = "3.91.152.225"
        port = "5432"
        database = "dw"
        user = "postgres"
        password = "ignacio"
        schema_name = "taller"

        # Conexión a la base de datos PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        # Crear un motor SQLAlchemy
        engine = create_engine(f'postgresql://{user}:{password}@{host}/{database}')

        # Guardar el DataFrame como una tabla en PostgreSQL en un esquema específico
        df.to_sql('mantenimiento_kilometros', engine, schema=schema_name, if_exists='replace', index=False)

        # Cerrar la conexión
        conn.close()

        return True
    except Exception as e:
        print(f"Error al subir datos a la base de datos: {e}")
        return False


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.initUI()

    def initUI(self):
        self.setWindowTitle('Subir Datos')
        self.setGeometry(100, 100, 300, 200)

        # Crear el botón
        btn_subir = QPushButton('Subir Datos', self)
        btn_subir.clicked.connect(self.subir_datos)

        # Layout
        layout = QVBoxLayout()
        layout.addWidget(btn_subir)

        self.setLayout(layout)

    def subir_datos(self):
        if subir_datos():
            QMessageBox.information(self, 'Éxito', 'Los datos se subieron correctamente a la base de datos.')
        else:
            QMessageBox.critical(self, 'Error', 'Hubo un problema al subir los datos a la base de datos.')


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
