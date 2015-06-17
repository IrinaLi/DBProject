#include <string>
#include <core/global_definitions.hpp>
#include <core/base_column.hpp>
#include <core/column_base_typed.hpp>
#include <core/column.hpp>
#include <core/compressed_column.hpp>

/*this is the include for the example compressed column with empty implementation*/
#include <compression/dictionary_compressed_column.hpp>
#include <compression/rle_compressed_column_.hpp>
#include <compression/bit_vector_compressed_column.hpp>


using namespace CoGaDB;

bool unittest(boost::shared_ptr<ColumnBaseTyped<int> > ptr);
bool unittest(boost::shared_ptr<ColumnBaseTyped<float> > ptr);
//bool unittest(boost::shared_ptr<ColumnBaseTyped<double> > ptr);
bool unittest(boost::shared_ptr<ColumnBaseTyped<std::string> > ptr);

int main(){

	/*create an object of your implemented column, and pass the smart pointer to the unittests*/

	// boost::shared_ptr<Column<int> > col (new Column<int>("int column",INT)); // irli 
	// boost::shared_ptr<RLECompressedColumn<int> > col (new RLECompressedColumn<int>("int column",INT)); // irli 
	 boost::shared_ptr<DictionaryCompressedColumn<int> > col (new DictionaryCompressedColumn<int>("int column",INT)); // irli 
	// boost::shared_ptr<BitVectorCompressedColumn<int> > col (new BitVectorCompressedColumn<int>("int column",INT)); // irli 

	if(!unittest(col)){
		std::cout << "At least one Unittest Failed!" << std::endl;	
		return -1;	
	}
	std::cout << "Unitests Passed!" << std::endl;

	 col->print();

// irli boost::shared_ptr<Column<float> > col_float (new Column<float>("float column",FLOAT));
   // boost::shared_ptr<RLECompressedColumn<float> > col_float (new RLECompressedColumn<float>("float column",FLOAT));
	boost::shared_ptr<DictionaryCompressedColumn<float> > col_float (new DictionaryCompressedColumn<float>("float column",FLOAT));
//	 boost::shared_ptr<BitVectorCompressedColumn<float> > col_float (new BitVectorCompressedColumn<float>("float column",FLOAT));
	if(!unittest(col_float)){
		std::cout << "At least one Unittest Failed!" << std::endl;	
		return -1;	
	}
	std::cout << "Unitests Passed!" << std::endl;

	// todo test for dict. compression
	boost::shared_ptr<DictionaryCompressedColumn<std::string> > col_string (new DictionaryCompressedColumn<std::string>("string column",VARCHAR));

// irli boost::shared_ptr<Column<std::string> > col_string (new Column<std::string>("string column",VARCHAR));
//  boost::shared_ptr<RLECompressedColumn<std::string> > col_string (new RLECompressedColumn<std::string>("string column",VARCHAR));
//  boost::shared_ptr<BitVectorCompressedColumn<std::string> > col_string (new BitVectorCompressedColumn<std::string>("string column",VARCHAR));
	if(!unittest(col_string)){
		std::cout << "At least one Unittest Failed!" << std::endl;	
		return -1;	
	}
	std::cout << "Unitests Passed!" << std::endl;


//	/****** BULK UPDATE TEST ******/
//	{
//		std::cout << "BULK UPDATE TEST..."; // << std::endl;
//		boost::shared_ptr<Column<int> > uncompressed_col (new Column<int>("int column",INT));
//		boost::shared_ptr<Column<int> > compressed_col (new Column<int>("int column",INT));
//		//boost::shared_ptr<DictionaryCompressedColumn<int> > compressed_col (new DictionaryCompressedColumn<int>("compressed int column",INT));


//		uncompressed_col->insert(reference_data.begin(),reference_data.end()); 
//		compressed_col->insert(reference_data.begin(),reference_data.end()); 

//		bool result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//		if(!result){ 
//			std::cerr << std::endl << "operator== TEST FAILED!" << std::endl;	
//			return false;
//		}
//		PositionListPtr tids (new PositionList());
//		int new_value=rand()%100;
//	   for(unsigned int i=0;i<10;i++){
//	 		tids->push_back(rand()%uncompressed_col->size());
//	   }
//		
//		uncompressed_col->update(tids,new_value); 
//		compressed_col->update(tids,new_value); 

//		result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//		if(!result){
//			 std::cerr << std::endl << "BULK UPDATE TEST FAILED!" << std::endl;	
//			 return false;	
//		}
//		std::cout << "SUCCESS"<< std::endl;	

//	}

//	/****** BULK DELETE TEST ******/
//	{
//		std::cout << "BULK DELETE TEST..."; // << std::endl;
//		boost::shared_ptr<Column<int> > uncompressed_col (new Column<int>("int column",INT));
//		boost::shared_ptr<Column<int> > compressed_col (new Column<int>("int column",INT));

//		//boost::shared_ptr<DictionaryCompressedColumn<int> > compressed_col (new DictionaryCompressedColumn<int>("compressed int column",INT));

//		uncompressed_col->insert(reference_data.begin(),reference_data.end()); 
//		compressed_col->insert(reference_data.begin(),reference_data.end()); 

//		bool result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//		if(!result){ 
//			std::cerr << std::endl << "operator== TEST FAILED!" << std::endl;	
//			return false;
//		}

//		PositionListPtr tids (new PositionList());

//	   for(unsigned int i=0;i<10;i++){
//	 		tids->push_back(rand()%uncompressed_col->size());
//	   }
//		
//		uncompressed_col->remove(tids); 
//		compressed_col->remove(tids); 

//		result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(uncompressed_col))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(compressed_col));
//		if(!result){
//			 std::cerr << "BULK DELETE TEST FAILED!" << std::endl;	
//			 return false;	
//		}
//		std::cout << "SUCCESS"<< std::endl;	

//	}


 return 0;
}


