#include <string>
#include <core/global_definitions.hpp>
#include <core/base_column.hpp>
#include <core/column_base_typed.hpp>
#include <core/column.hpp>
#include <core/compressed_column.hpp>

#include <compression/dictionary_compressed_column.hpp>

using namespace CoGaDB;

template<typename T>
const T get_rand_value() {
	return 0;
}

template<>
const int get_rand_value() {
	return rand() % 100;
}

template<>
const float get_rand_value() {
	return float(rand() % 10000) / 100;
}

template<>
const std::string get_rand_value() {
	std::string characterfield="abcdefghijklmnopqrstuvwxyz";

	std::string s;
	for(unsigned int i=0;i<10;i++){
		s.push_back( characterfield[rand() % characterfield.size()]);
	}
	return s;
}

template<class T>
void fill_column(boost::shared_ptr<ColumnBaseTyped<T>> col, std::vector<T>& reference_data) {
	for(unsigned int i = 0;i < reference_data.size(); i++){
		reference_data[i] = get_rand_value<T>();
	}

	for (unsigned int i = 0; i < reference_data.size(); i++) {
		col->insert(reference_data[i]);
	}
	std::cout << "Size in Bytes: " << col->getSizeinBytes() << std::endl;
}

template<class T>
bool equals(std::vector<T> reference_data, boost::shared_ptr<ColumnBaseTyped<T> > col) {
	for (unsigned int i = 0; i < reference_data.size(); i++) {
		T col_value = (*col)[i];
		if (reference_data[i] != col_value) {
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
				<< "' TID: '"<< i 
				<< "' Expected Value: '" << reference_data[i] 
				<< "' Actual Value: '" << col_value << "'" 
				<< std::endl;
			return false;
		}
	}
	return true;
}

template<class T>
bool test_column(boost::shared_ptr<ColumnBaseTyped<T>> col, std::vector<T>& reference_data) {
	/****** BASIC INSERT TEST ******/
	std::cout << "BASIC INSERT TEST: Filling column with data..."; // << std::endl;	
	//col->insert(reference_data.begin(),reference_data.end()); 
	
	if (reference_data.size() != col->size()) { 
		std::cout << "Fatal Error! In Unittest: invalid data size" << std::endl;
		return false;
	}

	//col->print(); /// todo to delete
	if (!equals(reference_data, col)) {
		std::cerr << "BASIC INSERT TEST FAILED!" << std::endl;
		return false;
	}

	std::cout << std::endl;

	std::cout << "SUCCESS" << std::endl;


	/****** VIRTUAL COPY CONSTRUCTOR TEST ******/
	std::cout << "VIRTUAL COPY CONSTRUCTOR TEST...";

	//boost::shared_ptr<DictionaryCompressedColumn<int> > compressed_col (new DictionaryCompressedColumn<int>("compressed int column",INT));
	//compressed_col->insert(reference_data.begin(),reference_data.end()); 

	ColumnPtr copy = col->copy();
	if(!copy) { 
		std::cerr << std::endl << "VIRTUAL COPY CONSTRUCTOR TEST FAILED!" << std::endl;	
		return false;
	}	
	bool result = *(boost::static_pointer_cast<ColumnBaseTyped<T>>(copy)) == *(boost::static_pointer_cast<ColumnBaseTyped<T>>(col));
	if (!result) { 
		std::cerr << std::endl << "VIRTUAL COPY CONSTRUCTOR TEST FAILED!" << std::endl;	
		return false;
	}	
	std::cout << "SUCCESS"<< std::endl;  


	/****** UPDATE TEST ******/
	TID tid = rand() % 100;
	T new_value = get_rand_value<T>();
	std::cout << "UPDATE TEST: Update value on Position '" << tid << "' to new value '" << new_value << "'..."; // << std::endl;

	reference_data[tid] = new_value;

	col->update(tid, new_value);

	//col->print(); /// todo to delete
	if (!equals(reference_data, col)) {
		std::cerr << "UPDATE TEST FAILED!" << std::endl;	
		return false;
	}
	std::cout << "SUCCESS"<< std::endl;


	/****** DELETE TEST ******/
	{
		TID tid = rand() % 100;

		std::cout << "DELETE TEST: Delete value on Position '" << tid << "'..."; // << std::endl;

		reference_data.erase(reference_data.begin()+tid);

		col->remove(tid);

		//col->print(); /// todo to delete
		if (!equals(reference_data, col)) {
			std::cerr << "DELETE TEST FAILED!" << std::endl;
			return false;
		}
		std::cout << "SUCCESS"<< std::endl;
	}

	/****** STORE AND LOAD TEST ******/
	{
		std::cout << "STORE AND LOAD TEST: store column data on disc and load it..."; // << std::endl;
		col->store("data/");

		col->clearContent();
		if(col->size() != 0) {
			std::cout << "Fatal Error! 'col->size()' returned non zero after call to 'col->clearContent()'\nTEST FAILED" << std::endl;
			return false;
		}

		//boost::shared_ptr<Column<int> > col2 (new Column<int>("int column",INT));
		col->load("data/");

		if (!equals(reference_data, col)) {
			std::cerr << "STORE AND LOAD TEST FAILED!" << std::endl;	
			return false;
		}
		std::cout << "SUCCESS"<< std::endl;
	}

	return true;
}
/****** CUSTOM UPDATE TEST ******/

template<class T>
bool test_column_update(boost::shared_ptr<ColumnBaseTyped<T>> col, std::vector<T>& reference_data, TID tid, T& value) 
{
	std::cout << "CUSTOM UPDATE TEST: Update value on Position '" << tid << "' to new value '" << value << "'..." << std::endl;

	reference_data[tid] = value;

	col->update(tid, value);

	if (!equals(reference_data, col)) {
		std::cerr << "UPDATE TEST FAILED!" << std::endl;	
		return false;
	}

	std::cout << "SUCCESS"<< std::endl;
	return true;
}
/******CUSTOM DELETE TEST ******/
template<class T>
bool test_column_delete(boost::shared_ptr<ColumnBaseTyped<T>> col, std::vector<T>& reference_data, TID tid){
	{

		std::cout << "CUSTOM DELETE TEST: Delete value on Position '" << tid << "'..."; // << std::endl;

		reference_data.erase(reference_data.begin()+tid);

		col->remove(tid);

		if (!equals(reference_data, col)) {
			std::cerr << "CUSTOM DELETE TEST FAILED!" << std::endl;
			return false;
		}
		std::cout << "SUCCESS"<< std::endl;
	}
	return true;
}

// additional update test
template<class T>
bool test_column_update(boost::shared_ptr<ColumnBaseTyped<T>> col, std::vector<T>& reference_data) {

	std::cout << "test update in the first position" << std::endl;
	test_column_update (col, reference_data, 0 , reference_data[1]);
	std::cout << "test update in the last position" << std::endl;
	test_column_update (col, reference_data, 99 , reference_data[98]);
	std::cout << "test update in the middle of colomn" << std::endl;
	test_column_update (col, reference_data, 13 , reference_data[14]);
	std::cout << "test three neighbour" << std::endl;
	test_column_update (col, reference_data, 55 , reference_data[54]);
	test_column_update (col, reference_data, 57 , reference_data[55]);
	test_column_update (col, reference_data, 56 , reference_data[55]);

	return true;
}

// additional delete test
template<class T>
bool test_column_delete(boost::shared_ptr<ColumnBaseTyped<T>> col, std::vector<T>& reference_data) {

	std::cout << "test update in the first position" << std::endl;
	test_column_delete (col, reference_data, 0);
	std::cout << "test update in the last position" << std::endl;
	test_column_delete (col, reference_data, 98);
	std::cout << "test update in the middle of colomn" << std::endl;
	test_column_delete (col, reference_data, 66);
	return true;
}

/* print uncompressed culomn */
template<class T>
void print_uncompressed_column(std::vector<T>& reference_data)
{
		std::cout << std::endl;
		std::cout << "| unompressed column | " << std::endl;
		for(unsigned int i = 0; i < reference_data.size(); i++)
		{

			std::cout <<" "<<"["<< i <<"]-"<< reference_data [i] << " ";

		}
		std::cout << std::endl;
		int size = reference_data.size() * sizeof(T);
		std::cout <<"size of uncompressed column in Bytes " << size << std::endl;
		std::cout << std::endl;
}

bool unittest(boost::shared_ptr<ColumnBaseTyped<int>> col) {
	std::cout << "RUN Unittest for Column with BaseType ColumnBaseTyped<int> >" << std::endl;
	
	std::vector<int> reference_data(100);

	fill_column(col, reference_data);

	// print_uncompressed_column(reference_data); // print uncompressed int column for testing
    // col->print(); // print compressed int column for testing

   	return test_column(col, reference_data);
	//  return test_column_update(col, reference_data); 
	//  return test_column_delete(col, reference_data); 
}

bool unittest(boost::shared_ptr<ColumnBaseTyped<float>> col) {
	std::cout << "RUN Unittest for Column with BaseType ColumnBaseTyped<float> >" << std::endl;

	std::vector<float> reference_data(100);

	fill_column(col, reference_data);

	// print_uncompressed_column(reference_data); // print uncompressed  column for testing
	// col->print(); // print compressed column for testing

	return test_column(col, reference_data);
}

bool unittest(boost::shared_ptr<ColumnBaseTyped<std::string>> col) {
	std::cout << "RUN Unittest for Column with BaseType ColumnBaseTyped<std::string> >" << std::endl;

	std::vector<std::string> reference_data(100);

	fill_column(col, reference_data);

	// print_uncompressed_column(reference_data); // print uncompressed  column for testing
	// col->print(); // print compressed column for testing

	return test_column(col, reference_data);
}