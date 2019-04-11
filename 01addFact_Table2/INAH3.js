//Path
var libroPath = './template.xlsx';

//Paquetes:
var azure = require('azure-storage');
const XLSX = require('xlsx');

//Crear conexión:
var azure2 = require('../keys_azure.js'); //Importación de llaves
var tableSvc = azure.createTableService(azure2.myaccount, azure2.myaccesskey);

//Leer Libro:
var workbook = XLSX.readFile(libroPath);
var sheet_name_list = workbook.SheetNames;
data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);

//Query:
var query = new azure.TableQuery()
    .where('Proyecto eq ?', 'INAH3');
var nextContinuationToken = null;

//Tabla origen:
var tablaUsar = "botdyesatb02";

//Variables:
var contador = 0;
var finalizar = false;

//JSON base
var task = {
    PartitionKey: { '_': '' },
    RowKey: { '_': '' },
    Timestamp: { '_': '' },
    Area: { '_': '' },
    Baja: { '_': '' },
    Borrado: { '_': '' },
    Check: { '_': '' },
    Descripcion: { '_': '' },
    Fecha_Fact: { '_': '' },
    Fecha_Fin: { '_': '' },
    Fecha_ini: { '_': '' },
    HojaDeServicio: { '_': '' },
    Inmueble: { '_': '' },
    Localidad: { '_': '' },
    No_Fact: { '_': '' },
    NombreEnlace: { '_': '' },
    NombreUsuario: { '_': '' },
    Pospuesto: { '_': '' },
    Proyecto: { '_': '' },
    Resguardo: { '_': '' },
    SerieBorrada: { '_': '' },
    Servicio: { '_': '' },
    Status: { '_': '' },
};


//Programa
async function working() {

    //Reiniciar token:
    nextContinuationToken = null

    //Bucle:
    do {
        await promesa();
    } while (finalizar == false);
    resultado();
}

//Programa:
function promesa() {
    return new Promise(function(resolve, reject) {

        tableSvc.queryEntities(tablaUsar, query, nextContinuationToken, function(error, results, response) {
            if (!error) {

                //Bucle que analiza Azure Table:
                results.entries.forEach(function(entry) {
                    //Bucle que analiza Excel:
                    for (var key in data) {
                        if (entry['RowKey']['_'] == `${data[key]['Serie']}`) {
                            console.log(`Coincide ${entry['RowKey']['_']} - ${data[key]['Serie']}`);

                            //Tomamos la entidad y la guardamos en el JSON base:
                            task['PartitionKey']['_'] = entry['PartitionKey']['_']; // 1
                            task['RowKey']['_'] = entry['RowKey']['_']; //2
                            //task['Timestamp']['_'] = entry['Timestamp']['_']; //3
                            //-----------4
                            if (entry['Area'] == undefined) {
                                task['Area']['_'] = "";
                            } else {
                                task['Area']['_'] = entry['Area']['_'];
                            }
                            //-----------5
                            if (entry['Baja'] == undefined) {
                                task['Baja']['_'] = "";
                            } else {
                                task['Baja']['_'] = entry['Baja']['_'];
                            }
                            //-----------6
                            if (entry['Borrado'] == undefined) {
                                task['Borrado']['_'] = "";
                            } else {
                                task['Borrado']['_'] = entry['Borrado']['_'];
                            }
                            //-----------7
                            if (entry['Check'] == undefined) {
                                task['Check']['_'] = "";
                            } else {
                                task['Check']['_'] = entry['Check']['_'];
                            }
                            //-----------8
                            if (entry['Descripcion'] == undefined) {
                                task['Descripcion']['_'] = "";
                            } else {
                                task['Descripcion']['_'] = entry['Descripcion']['_'];
                            }
                            //-----------9
                            if (entry['Fecha_Fin'] == undefined) {
                                task['Fecha_Fin']['_'] = "";
                            } else {
                                task['Fecha_Fin']['_'] = entry['Fecha_Fin']['_'];
                            }
                            //-----------10
                            if (entry['Fecha_ini'] == undefined) {
                                task['Fecha_ini']['_'] = "";
                            } else {
                                task['Fecha_ini']['_'] = entry['Fecha_ini']['_'];
                            }
                            //-----------11
                            if (entry['HojaDeServicio'] == undefined) {
                                task['HojaDeServicio']['_'] = "";
                            } else {
                                task['HojaDeServicio']['_'] = entry['HojaDeServicio']['_'];
                            }
                            //-----------12
                            if (entry['Inmueble'] == undefined) {
                                task['Inmueble']['_'] = "";
                            } else {
                                task['Inmueble']['_'] = entry['Inmueble']['_'];
                            }
                            //-----------13
                            if (entry['Localidad'] == undefined) {
                                task['Localidad']['_'] = "";
                            } else {
                                task['Localidad']['_'] = entry['Localidad']['_'];
                            }
                            //-----------14
                            if (entry['NombreEnlace'] == undefined) {
                                task['NombreEnlace']['_'] = "";
                            } else {
                                task['NombreEnlace']['_'] = entry['NombreEnlace']['_'];
                            }
                            //-----------15
                            if (entry['NombreUsuario'] == undefined) {
                                task['NombreUsuario']['_'] = "";
                            } else {
                                task['NombreUsuario']['_'] = entry['NombreUsuario']['_'];
                            }
                            //-----------16
                            if (entry['Pospuesto'] == undefined) {
                                task['Pospuesto']['_'] = "";
                            } else {
                                task['Pospuesto']['_'] = entry['Pospuesto']['_'];
                            }
                            //-----------17
                            if (entry['Proyecto'] == undefined) {
                                task['Proyecto']['_'] = "";
                            } else {
                                task['Proyecto']['_'] = entry['Proyecto']['_'];
                            }
                            //-----------18
                            if (entry['Resguardo'] == undefined) {
                                task['Resguardo']['_'] = "";
                            } else {
                                task['Resguardo']['_'] = entry['Resguardo']['_'];
                            }
                            //-----------19
                            if (entry['SerieBorrada'] == undefined) {
                                task['SerieBorrada']['_'] = "";
                            } else {
                                task['SerieBorrada']['_'] = entry['SerieBorrada']['_'];
                            }
                            //-----------20
                            if (entry['Servicio'] == undefined) {
                                task['Servicio']['_'] = "";
                            } else {
                                task['Servicio']['_'] = entry['Servicio']['_'];
                            }
                            //-----------21
                            if (entry['Status'] == undefined) {
                                task['Status']['_'] = "";
                            } else {
                                task['Status']['_'] = entry['Status']['_'];
                            }
                            //-----------22
                            if (entry['No_Fact'] == undefined) {
                                task['No_Fact']['_'] = "";
                            } else {
                                if (data[key]['No_Factura'] == null) {
                                    task['No_Fact']['_'] = "";
                                } else {
                                    task['No_Fact']['_'] = data[key]['No_Factura'];
                                }
                            }
                            //-----------23
                            if (entry['Fecha_Fact'] == undefined) {
                                task['Fecha_Fact']['_'] = "";
                            } else {
                                if (data[key]['Fecha'] == null) {
                                    task['Fecha_Fact']['_'] = "";
                                } else {
                                    task['Fecha_Fact']['_'] = data[key]['Fecha'];
                                }
                            }

                            //Remplazamos la entidad con la nueva base modificada:
                            tableSvc.replaceEntity(tablaUsar, task, function(error, result, response) {
                                if (!error) {
                                    console.log("La entidad se modifico correctamente.");
                                }
                            });

                            //Aumentamos la celda para trabajar en la siguiente y sumamos un contador para conocer
                            //el resultado por log:
                            contador++;
                        }
                    }
                });
            }

            //Token que permite continuar despues de leer 1000 rows:
            if (results.continuationToken) {
                nextContinuationToken = results.continuationToken;
                resolve();
            } else {
                finalizar = true;
                resolve();
            }

        });
    });
}

//Funcion que se ejecuta el final del programa:
function resultado() {
    console.log(`Se encontraron ${contador} coincidencias.`);
}

//Inicia el trabajo:
working();