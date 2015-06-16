
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */	
template<class T>
class DictionaryCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	DictionaryCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~DictionaryCompressedColumn();

	virtual bool insert(const boost::any& new_Value);
	virtual bool insert(const T& new_value);
	template <typename InputIterator>
	bool insert(InputIterator first, InputIterator last);

	virtual bool update(TID tid, const boost::any& new_value);
	virtual bool update(PositionListPtr tid, const boost::any& new_value);	
	
	virtual bool remove(TID tid);
	//assumes tid list is sorted ascending
	virtual bool remove(PositionListPtr tid);
	virtual bool clearContent();

	virtual const boost::any get(TID tid);
	//virtual const boost::any* const getRawData()=0;
	virtual void print() const throw();
	virtual size_t size() const throw();
	virtual unsigned int getSizeinBytes() const throw();

	virtual const ColumnPtr copy() const;

	virtual bool store(const std::string& path);
	virtual bool load(const std::string& path);
	
	
	virtual T& operator[](const int index);
	
private:
	std::vector< std::pair<int,T> > dictionary_vector; // holds references of specific data
	std::vector< int > compressed_vector; // val_vector
};


/***************** Start of Implementation Section ******************/

	
	template<class T>
	DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type){

	}

	template<class T>
	DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(){

	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const boost::any&){
		return false;
	}

	// todo 
	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const T& new_value){

		// if empty check dictionary and insert new type
		if(compressed_vector.empty()){
			dictionary_vector.push_back(std::make_pair(0, new_value)); // zero based counter (?)
			compressed_vector.push_back(0);
		} 
		else
		{
			bool found = false;
			unsigned int i = 0;
			for ( i ; i < dictionary_vector.size(); i++)
			{
				if(dictionary_vector[i].second  == new_value) // already stored in dictionary return index
				{		
					compressed_vector.push_back(dictionary_vector[i].first); // push reference key at the end of the vector
					found = true;
				}
			
			}
			/**
			* if no element was found in dictionary
			* insert into dirctionary and return new reference key of new entry
			* reference key is vector_size-1
			**/ 
			if(!found)
			{
				dictionary_vector.push_back(std::make_pair(i, new_value)); // on the end insert
				unsigned int extend_index = dictionary_vector[i].first; // get index
				compressed_vector.push_back(extend_index);
			}
		}


		return true;
	}

	template <typename T> 
	template <typename InputIterator>
	bool DictionaryCompressedColumn<T>::insert(InputIterator , InputIterator ){
		
		return true;
	}

	template<class T>
	const boost::any DictionaryCompressedColumn<T>::get(TID){

		return boost::any();
	}

	// todo
	template<class T>
	void DictionaryCompressedColumn<T>::print() const throw(){
		std::cout << "| " << this->name_ << " |" << std::endl;
		std::cout << "________________________" << std::endl;
		
		unsigned int dict_index  = 0;
		
		for(unsigned int i = 0;i < compressed_vector.size(); i++)
		{
			dict_index = compressed_vector[i]; // todo direct access to dct vector also possible?
			for(unsigned int j = 0; j< dictionary_vector.size(); j++)
			{
				if(dictionary_vector[j].first == dict_index ){
					T dict_element = dictionary_vector[j].second;
					std::cout << i << ": " << dict_index << "| " << dict_element  << std::endl;
					break;
				}
			}
		}	
	}

	// todo
	template<class T>
	size_t DictionaryCompressedColumn<T>::size() const throw(){
		return compressed_vector.size(); 
	}

	// todo
	template<class T>
	const ColumnPtr DictionaryCompressedColumn<T>::copy() const{		
		return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
	}

	// todo
	template<class T>
	bool DictionaryCompressedColumn<T>::update(TID , const boost::any& ){
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(PositionListPtr , const boost::any& ){
		return false;		
	}
	
	// todo
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(TID){
		return false;	
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(PositionListPtr){
		return false;			
	}

	// todo
	template<class T>
	bool DictionaryCompressedColumn<T>::clearContent(){
		// compressed_vector.clear();
		return false;
	}


	// todo
	template<class T>
	bool DictionaryCompressedColumn<T>::store(const std::string& path_){
		//string path("data/");
		std::string path(path_);
		path += "/";
		path += this->name_;
		//std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << compressed_vector;

		outfile.flush();
		outfile.close();
		return true;
	}

	// todo
	template<class T>
	bool DictionaryCompressedColumn<T>::load(const std::string& path_){
		std::string path(path_);
		//std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
		//string path("data/");
		path += "/";
		path += this->name_;
		
		//std::cout << "Opening File '" << path << "'..." << std::endl;
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);

		ia >> compressed_vector;
		infile.close(); 
		return false;
	}

	// todo
	template<class T>
	T& DictionaryCompressedColumn<T>::operator[](const int index){		
		static T t;		

		unsigned int dict_index  = 0;
		
		for(unsigned int i = 0;i < compressed_vector.size(); i++)
		{
			dict_index = compressed_vector[i]; // todo direct access to dct vector also possible?
			for(unsigned int j = 0; j< dictionary_vector.size(); j++)
			{
				if(dictionary_vector[j].first == dict_index ){					
					t = dictionary_vector[j].second;
					break;
				}
			}
		}	

		return t;
	}

	// todo
	template<class T>
	unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw(){
		// old: return 0; //return values_.capacity()*sizeof(T);
		return compressed_vector.capacity() * sizeof(int);
	}
	
/***************** End of Implementation Section ******************/

}; //end namespace CogaDB

