using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using System.Data.Odbc;

namespace MAXI_BROKER
{
    class Program
    {

        static string connectionString =
                "User=SYSDBA;" +
                "Password=masterkey;" +
                "Database=C:\\Users\\MKMZ9_000\\Documents\\RZAL\\KBBAZA.fdb;" +
                "DataSource=localhost;" +
                "Port=3050;";

        static string odbcConfig = "DRIVER={DBISAM 4 ODBC Driver};ConnectionType=Local;CatalogName=C:\\AGENT.m6\\baza";

        static string prepareString(object item)
        {
            return (item.ToString().Trim() == "") ? "''" : String.Format("'{0}'", item.ToString().Trim().Replace("'", "''"));
        }

        static string prepareNumber(object item)
        {
            return (item.ToString().Trim() == "") ? "NULL" : item.ToString().Trim();
        }

        static string prepareDate(object item)
        {
            if (item.ToString().Trim() == "")
            {
                return null;
            }
            char[] sep = new char[] { '/', ' ' };
            string[] tabDate = item.ToString().Trim().Split(sep);
            return String.Format("'{2}-{1}-{0}'", tabDate[0], tabDate[1], tabDate[2]);
        }

        static bool odbcCheckIsExists(string statement, OdbcCommand odbcCmd)
        {
            odbcCmd.CommandText = statement;
            OdbcDataReader odbcReader = odbcCmd.ExecuteReader();
            bool isMarka = false;
            while (odbcReader.Read())
            {
                isMarka = true;
                break;
            }
            odbcReader.Close();
            return isMarka;
        }

        static void odbcMakeTransaction(string statement, OdbcCommand odbcCmd)
        {
            using (OdbcTransaction odbcTrans = odbcCmd.Connection.BeginTransaction())
            {
                odbcCmd.CommandText = statement;
                odbcCmd.Transaction = odbcTrans;
                odbcCmd.ExecuteNonQuery();
                odbcTrans.Commit();
            }

        }

        static int odbcGetId(string statement, OdbcCommand odbcCmd)
        {
            odbcCmd.CommandText = statement;
            return (int)odbcCmd.ExecuteScalar();
        }

        static void transferCars()
        {

            FbConnection fbCon = new FbConnection(connectionString);
            fbCon.Open();
            OdbcConnection odbcCon = new OdbcConnection(odbcConfig);
            odbcCon.Open();
            
            FbCommand myCmd = new FbCommand();
            myCmd.CommandText = "SELECT * FROM POJAZDY;";
            myCmd.Connection = fbCon;

            FbDataReader fbReader = myCmd.ExecuteReader();
            while (fbReader.Read())
            {

                string nrRej = prepareString(fbReader["NR_REJ"]);
                string nrDowodu = prepareString(fbReader["NUMER_KARTY"]);
                string nrNadwozia = prepareString(fbReader["NR_NADWOZIA"]);
                string nrSilnika = prepareString(fbReader["NR_SILNIKA"]);
                string rokProdukcji = prepareNumber(fbReader["ROK_PROD"]);
                string moc = prepareNumber(fbReader["MOC_KM"]);
                string dataRej = prepareDate(fbReader["DATA_REJESTRACJI"]);
                string dataBadan = prepareDate(fbReader["DATA_BADANIA"]);
                string marka = prepareString(fbReader["MARKA"]);
                string model = prepareString(fbReader["MODEL"]);
                string typModelu = prepareString(fbReader["RODZAJ"]);
                string pojemnosc = prepareString(fbReader["POJEMNOSC"]);
                string ladownosc = prepareString(fbReader["LADOWNOSC"]);
                string ilosc_miejsc = prepareString(fbReader["MIEJSC"]);

                using (OdbcCommand odbcCmd = new OdbcCommand())
                {
                    odbcCmd.Connection = odbcCon;

                    bool isMarka = odbcCheckIsExists(String.Format("SELECT NAZWA FROM Marki WHERE NAZWA = {0}", marka), odbcCmd);
                    
                    if (!isMarka)
                    {
                        odbcMakeTransaction(String.Format("INSERT INTO MARKI (NAZWA) VALUES ({0})", marka), odbcCmd);
                    }

                    int idMarki = odbcGetId(String.Format("SELECT id FROM MARKI WHERE NAZWA = {0}", marka), odbcCmd);

                    bool isModel = odbcCheckIsExists(String.Format("SELECT 1 FROM Modele WHERE ID_MARKI = {0} AND NAZWA = {1} ", idMarki.ToString(), model), odbcCmd);

                    if (!isModel)
                    {

                        odbcMakeTransaction(String.Format("INSERT INTO MODELE (ID_MARKI, NAZWA, TYP, POJEMNOSC, LADOWNOSC, ILOSC_MIEJSC) " +
                                "VALUES ({0}, {1}, {2}, {3}, {4}, {5})", idMarki, model, typModelu, pojemnosc, ladownosc, ilosc_miejsc), odbcCmd);

                    }

                    int idModelu = odbcGetId(String.Format("SELECT 1 FROM Modele WHERE ID_MARKI = {0} AND NAZWA = {1} ", idMarki.ToString(), model), odbcCmd);


                    bool isTyp = odbcCheckIsExists(String.Format("SELECT 1 FROM Auto_typ WHERE NAZWA = {0} ", typModelu), odbcCmd);
                    if (!isTyp)
                    {

                        odbcMakeTransaction(String.Format("INSERT INTO Auto_typ (NAZWA) VALUES ({0})", typModelu), odbcCmd);

                    }

                    int idTyp = odbcGetId(String.Format("SELECT 1 FROM Auto_typ WHERE NAZWA = {0} ", typModelu), odbcCmd);

                    odbcMakeTransaction(String.Format("INSERT INTO Auta2 (id_Model, nr_rej, nr_dow_rej, nr_nadwozia, nr_silnika, " +
                            "rok_produkcji, id_marka, pojemnosc, ladownosc, id_rodzaj, moc) VALUES " +
                            "( {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10} )",
                            idModelu, nrRej, nrDowodu, nrNadwozia, nrSilnika, rokProdukcji, idMarki, pojemnosc, ladownosc, idTyp, moc), odbcCmd);

                    int idAuto = odbcGetId(String.Format("SELECT id FROM Auta2 WHERE id_Model = {0} AND nr_rej = {1} AND nr_dow_rej = {2} AND nr_nadwozia = {3} " +
                        "AND nr_silnika = {4} ", idModelu, nrRej, nrDowodu, nrNadwozia, nrSilnika), odbcCmd);

                    if (dataBadan != null)
                    {
                        odbcMakeTransaction(String.Format("UPDATE Auta2 SET data_badan = {0} WHERE id = {1} ", dataBadan, idAuto), odbcCmd);
                    }

                    if (dataRej != null)
                    {
                        odbcMakeTransaction(String.Format("UPDATE Auta2 SET data_rej = {0} WHERE id = {1} ", dataRej, idAuto), odbcCmd);
                    }


                }
            }
            odbcCon.Close();
            fbCon.Close();
        }

        static void transferWorkers()
        {

            FbConnection fbCon = new FbConnection(connectionString);
            fbCon.Open();
            OdbcConnection odbcCon = new OdbcConnection(odbcConfig);
            odbcCon.Open();


            FbCommand fbCmd = new FbCommand();
            fbCmd.CommandText = "SELECT * FROM BROKERZY;";
            fbCmd.Connection = fbCon;

            OdbcCommand odbcCmd = new OdbcCommand();
            odbcCmd.Connection = odbcCon;

            FbDataReader fbReader = fbCmd.ExecuteReader();
            while (fbReader.Read())
            {
                string Imie = prepareString(fbReader["IMIE"]);
                string Nazwisko = prepareString(fbReader["NAZWISKO"]);
                string Uwagi = prepareString(fbReader["UWAGI"]);


                odbcMakeTransaction(String.Format("INSERT INTO osoby (Imie, Nazwisko, Uwagi) VALUES ({0}, {1}, {2})", Imie, Nazwisko, Uwagi), odbcCmd);

            }

            odbcCon.Close();
            fbCon.Close();
        }


        static void transferPersons()
        {

            FbConnection fbCon = new FbConnection(connectionString);
            fbCon.Open();
            OdbcConnection odbcCon = new OdbcConnection(odbcConfig);
            odbcCon.Open();


            FbCommand fbCmd = new FbCommand();
            fbCmd.CommandText = "SELECT * FROM KLIENCI;";
            fbCmd.Connection = fbCon;

            OdbcCommand odbcCmd = new OdbcCommand();
            odbcCmd.Connection = odbcCon;

            FbDataReader fbReader = fbCmd.ExecuteReader();
            while (fbReader.Read())
            {
                string idOs = "100" + prepareNumber(fbReader["ID"]);
                string Regon = prepareString(fbReader["REGON"]);
                string Pesel = prepareString(fbReader["PESEL"]);
                string tel1 = prepareString(fbReader["TELEFON_CENTRALI"]);
                string tel2 = prepareString(fbReader["TELEFON_2"]);
                string telKom = prepareString(fbReader["TELEFON_3"]);
                string email = prepareString(fbReader["EMAIL"]);
                string nip = prepareString(fbReader["NIP"]);
                string isCompany = "True";
                string Notatki = prepareString(fbReader["UWAGI"]);

                string zarzad = prepareString(fbReader["ZARZAD"]);


                odbcMakeTransaction(String.Format("INSERT INTO osoby (id, Regon, Pesel, Tel, Tel2, Tel_kom, email, nip, notatki, firma) " +
                    "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9} )",idOs, Regon, Pesel, tel1, tel2, telKom, email, nip, Notatki, isCompany), odbcCmd);

                
                
                
                if(zarzad != "''")
                {
                    string[] zarzadTab = zarzad.Replace("'", "").Replace("  ", " ").Split(new char[] { ' ', '\t' });

                    odbcMakeTransaction(String.Format("INSERT INTO wspol (id_os, imie, nazwisko, pesel, regon, nip) VALUES " +
                    "({0}, {1}, {2}, {3}, {4}, {5})", idOs, String.Format("'{0}'", zarzadTab[0]), String.Format("'{0}'", zarzadTab[1]), Pesel, Regon, nip),
                    odbcCmd);


                }


            }

            odbcCon.Close();
            fbCon.Close();
        }

        static void transferInsurances()
        {

            FbConnection fbCon = new FbConnection(connectionString);
            fbCon.Open();
            OdbcConnection odbcCon = new OdbcConnection(odbcConfig);
            odbcCon.Open();

            FbCommand fbCmd = new FbCommand();
            fbCmd.CommandText = "SELECT * FROM KPIR;";
            fbCmd.Connection = fbCon;

            OdbcCommand odbcCmd = new OdbcCommand();
            odbcCmd.Connection = odbcCon;

            FbDataReader fbReader = fbCmd.ExecuteReader();
            while (fbReader.Read())
            {
                string nrDok = prepareString(fbReader["NR_DOKUMENTU"]);
                string fbIdFakt = prepareString(fbReader["ID"]);
                string bruttoSum = prepareNumber(fbReader["KWOTA"]);
                string nettoSum = prepareNumber(fbReader["KWOTA_NETTO"]);
                string vatSum = (Convert.ToDouble(bruttoSum) - Convert.ToDouble(nettoSum)).ToString();
                string zapl = Convert.ToBoolean(prepareNumber(fbReader["ROZLICZONA"])) ? "'TAK'" : "'NIE'";
                string dataFakt = prepareDate(fbReader["DATA"]);
                string dataSprze = prepareDate(fbReader["DATA_SPRZEDARZY"]);
                string dataPlatnosci = prepareDate(fbReader["DATA_PLATNOSCI"]);

                odbcMakeTransaction(String.Format("INSERT INTO faktury (numer, brutto, netto, vat) VALUES ({0}, {1}, {2}, {3})", nrDok, bruttoSum, nettoSum, vatSum),
                    odbcCmd);

                int idFakt = odbcGetId(String.Format("SELECT id FROM faktury WHERE numer = {0} AND brutto = {1}", nrDok, bruttoSum), odbcCmd);

                if (dataFakt != null)
                {
                    odbcMakeTransaction(String.Format("UPDATE faktury SET data = {0} WHERE id = {1} ", dataFakt, idFakt), odbcCmd);
                }
                if (dataSprze != null)
                {
                    odbcMakeTransaction(String.Format("UPDATE faktury SET data_sprze = {0} WHERE id = {1} ", dataSprze, idFakt), odbcCmd);
                }
                if (dataPlatnosci != null)
                {
                    odbcMakeTransaction(String.Format("UPDATE faktury SET data_platnosci = {0} WHERE id = {1} ", dataPlatnosci, idFakt), odbcCmd);
                }

                FbCommand fbCmd1 = new FbCommand();
                fbCmd1.CommandText = String.Format("SELECT * FROM FAKTURY_POZYCJE WHERE ID_KPIR = {0};", fbIdFakt.ToString());
                fbCmd1.Connection = fbCon;
                FbDataReader fbReader1 = fbCmd1.ExecuteReader();
                while (fbReader1.Read())
                {
                    double brutto = Convert.ToDouble(prepareNumber(fbReader1["CENA_BRUTTO"]));
                    double netto = Convert.ToDouble(prepareNumber(fbReader1["CENA_NETTO"]));
                    double vat = brutto - netto;
                    string stawkaVat = prepareNumber(fbReader1["VAT"]);
                    if (stawkaVat == "NULL" || stawkaVat == "-1")
                    {
                        stawkaVat = "0";
                    }
                    string nazwaPoz = prepareString(fbReader1["OPIS"]);
                    string jMiary = prepareString(fbReader1["JM"]);
                    string ilosc = prepareNumber(fbReader1["ILOSC"]);
                    string skw = prepareString(fbReader1["SWW"]);

                    odbcMakeTransaction(String.Format("INSERT INTO fakt_poz (id_fakt, nazwa, jm, ilosc, netto, " +
                        "wartosc_netto, stawka_vat, vat, wartosc_brutto, skw) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9} )",
                        idFakt, nazwaPoz, jMiary, ilosc, netto, netto, stawkaVat, vat, brutto, skw ), odbcCmd);

                    


                }


            }
            odbcCon.Close();
            fbCon.Close();
        }


        static void Main(string[] args)
        {
            transferPersons();
            //transferCars();
            //transferInsurances();

            Console.WriteLine("Transfer successfully finished");
            Console.ReadKey();
        }
    }

}
