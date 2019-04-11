//Paquetes:
var azure = require('azure-storage');
var nodeoutlook = require('nodejs-nodemailer-outlook');
var Excel = require('exceljs');

//Crear conexión:
var azure2 = require('../keys_azure'); //Importación de llaves
var tableSvc = azure.createTableService(azure2.myaccount, azure2.myaccesskey);

//Tabla origen:
var tablaUsar = "botdyesatb05";
var tablaActualizar = "botdyesatb02";

//Variables:
var task;
var contador = 0;
var contadorSeriesMax = 1;
var finalizar = false;
var proyecto = "OperacionInterna";

//Crear Libro Final:
var workbookFinal = new Excel.Workbook('algo');
var worksheet = workbookFinal.addWorksheet('Hoja1');
var celdaActual = 1;

//Query:
var query = new azure.TableQuery()
    .where('PartitionKey eq ?', `${proyecto}`);
var nextContinuationToken = null;

//Programa
async function working() {

    //Reiniciar token:
    nextContinuationToken = null;

    //Excel:
    worksheet.getCell(`A${celdaActual}`).value = 'Asociado';
    worksheet.getCell(`B${celdaActual}`).value = 'Serie';
    worksheet.getCell(`C${celdaActual}`).value = 'Proyecto';
    celdaActual++;

    //Bucle:
    do {
        await promesa();
    } while (finalizar == false);
    resultado();
}

function promesa() {
    return new Promise(function(resolve, reject) { //Promesa 1

        //Blucle Tabla5:
        tableSvc.queryEntities(tablaUsar, query, nextContinuationToken, function(error, results, response) {
            if (!error) {
                results.entries.forEach(function(entry) {
                    if (entry['No_Fact']['_'] == "") {
                        if (contadorSeriesMax <= 100) {

                            //ObtenerJSON:
                            tableSvc.retrieveEntity(tablaActualizar, entry['Asociado']['_'], entry['RowKey']['_'], function(errorRetrieve, resultRetrieve, responseRetrieve) {
                                if (!errorRetrieve) {
                                    task = resultRetrieve;
                                    console.log("JSON obtenido.");
                                    task['Status']['_'] = 'Por_Facturar';

                                    //Actualizar entidad:
                                    tableSvc.replaceEntity(tablaActualizar, task, function(errorReplace, resultReplace, responseReplace) {
                                        if (!errorReplace) {
                                            console.log("Entidad por facturar actualizada.");
                                        }
                                    });
                                }
                            });

                            //Escribir entidad en Excel:
                            worksheet.getCell(`A${celdaActual}`).value = entry['Asociado']['_'];
                            worksheet.getCell(`B${celdaActual}`).value = entry['RowKey']['_'];
                            worksheet.getCell(`C${celdaActual}`).value = entry['PartitionKey']['_'];
                            celdaActual++;
                            contadorSeriesMax++;
                        }
                    }
                    //Contador de entidades analizadas:
                    contador++;
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

//Enviar Email:
async function sendMail() {
    return new Promise(function(resolve, reject) {
        nodeoutlook.sendEmail({
            auth: {
                user: azure2.email,
                pass: azure2.passEmail
            },
            from: azure2.email,
            to: "lrosas@mainbit.com.mx",
            subject: azure2.asunto,
            html: azure2.correohtml,
            attachments: [{
                path: './series.xlsx'
            }, ],
            onError: (e) => console.log(e),
            onSuccess: (i) => console.log(i)
        });
        console.log("Correo enviado.");
        resolve();
    });
}

//Función para guardar el libro creado con los datos extraidos por el programa:
async function guardarExcel() {
    return new Promise(function(resolve, reject) {
        workbookFinal.xlsx.writeFile('series.xlsx').then(function() { //Puedes colocar cualquier nombre al archivo final sustituyendo "final.xlsx" (recuerda respetar siempre la extención .xlsx).
            console.log("¡El archivo se a creado correctamente!");
            resolve();
        });
    });
}


//Funcion que se ejecuta el final del programa:
async function resultado() {
    //sendMail();
    console.log(`Se analizaron ${contador} entidades y se enviara un correo con la información de las primeras ${contadorSeriesMax - 1}.`);

    await guardarExcel();
    await sendMail();

}

//Inicia el trabajo:
working();