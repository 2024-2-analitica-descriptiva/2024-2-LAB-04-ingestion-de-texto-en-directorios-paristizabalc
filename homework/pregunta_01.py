# pylint: disable=import-outside-toplevel
# pylint: disable=line-too-long
# flake8: noqa
"""
Escriba el codigo que ejecute la accion solicitada en cada pregunta.
"""
import csv
import fileinput
import glob
import os
import zipfile


def pregunta_01():
    """
    La información requerida para este laboratio esta almacenada en el
    archivo "files/input.zip" ubicado en la carpeta raíz.
    Descomprima este archivo.

    Como resultado se creara la carpeta "input" en la raiz del
    repositorio, la cual contiene la siguiente estructura de archivos:


    ```
    train/
        negative/
            0000.txt
            0001.txt
            ...
        positive/
            0000.txt
            0001.txt
            ...
        neutral/
            0000.txt
            0001.txt
            ...
    test/
        negative/
            0000.txt
            0001.txt
            ...
        positive/
            0000.txt
            0001.txt
            ...
        neutral/
            0000.txt
            0001.txt
            ...
    ```

    A partir de esta informacion escriba el código que permita generar
    dos archivos llamados "train_dataset.csv" y "test_dataset.csv". Estos
    archivos deben estar ubicados en la carpeta "output" ubicada en la raiz
    del repositorio.

    Estos archivos deben tener la siguiente estructura:

    * phrase: Texto de la frase. hay una frase por cada archivo de texto.
    * sentiment: Sentimiento de la frase. Puede ser "positive", "negative"
      o "neutral". Este corresponde al nombre del directorio donde se
      encuentra ubicado el archivo.

    Cada archivo tendria una estructura similar a la siguiente:

    ```
    |    | phrase                                                                                                                                                                 | target   |
    |---:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------|
    |  0 | Cardona slowed her vehicle , turned around and returned to the intersection , where she called 911                                                                     | neutral  |
    |  1 | Market data and analytics are derived from primary and secondary research                                                                                              | neutral  |
    |  2 | Exel is headquartered in Mantyharju in Finland                                                                                                                         | neutral  |
    |  3 | Both operating profit and net sales for the three-month period increased , respectively from EUR16 .0 m and EUR139m , as compared to the corresponding quarter in 2006 | positive |
    |  4 | Tampere Science Parks is a Finnish company that owns , leases and builds office properties and it specialises in facilities for technology-oriented businesses         | neutral  |
    ```


    """

    # Leer registros
    def load_files(input_directory):

        input_directory = os.path.join(input_directory, "*")
        files = glob.glob(input_directory)
        with fileinput.input(files=files) as f:
            for line in f:
                generator = (
                    yield line,
                    f.filename().split("/")[-2],
                )

        return generator

    # Exportar a CSV
    def export_to_csv(data, output_directory, filename):

        def create_output_directory(output_directory) -> None:
            """Vidalación de la existencia del archivo y creación de la carpeta"""

            folders = glob.glob(f"{output_directory}/*")
            if len(folders) >= 2:
                for file in folders:
                    os.remove(file)
                os.rmdir(output_directory)

            os.makedirs(output_directory, exist_ok=True)

        def save_csv(data, output_directory, filename):
            """Guarda la información en un archivo CSV"""

            fieldnames = ["phrase", "target"]
            file_path = os.path.join(output_directory, filename)
            with open(file_path, "w") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in data:
                    writer.writerow({"phrase": row[0], "target": row[1]})

        create_output_directory(output_directory)
        save_csv(data, output_directory, filename)

    # Procesar archivos
    def run_job(zip_file, output_directory):
        """Ejecuta el proceso"""

        def unzip_files(zip_file):
            """Descomprime los archivos"""
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                # Extract all the contents to the specified directory
                zip_ref.extractall(zip_file.split("/")[0])

        def process_files(input_directory, output_directory):
            """Procesa los archivos"""
            for folders in glob.glob(f"{input_directory}/*"):
                data = []
                for folder in glob.glob(folders + "/*"):
                    sequence = load_files(folder)
                    data.extend(sequence)

                filename = f"{folders.split('/')[-1]}_dataset.csv"
                export_to_csv(data, output_directory, filename)

        unzip_files(zip_file)
        input_directory = zip_file.split(".")[0]
        process_files(input_directory, output_directory)

    run_job("files/input.zip", "files/output")

    return "Proceso finalizado"


if __name__ == "__main__":
    print(pregunta_01())