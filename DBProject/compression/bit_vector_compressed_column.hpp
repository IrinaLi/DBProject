
/*!
*/

#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a bit vector compressed column with type T.
 */	
template<class T>
class BitVectorCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	BitVectorCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~BitVectorCompressedColumn();

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

	std::vector< std::pair<T, std::vector<bool>> > val_vector;

};


/***************** Start of Implementation Section ******************/

	
	template<class T>
	BitVectorCompressedColumn<T>::BitVectorCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type){

	}

	template<class T>
	BitVectorCompressedColumn<T>::~BitVectorCompressedColumn(){

	}

	template<class T>
	bool BitVectorCompressedColumn<T>::insert(const boost::any&){

		return false;
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::insert(const T& new_value){
		std::vector<bool> bit_vector;
		if(val_vector.empty()) 
		{
			bit_vector.push_back(true);
			val_vector.push_back(std::make_pair(new_value, bit_vector));
		}
		else
		{
			bool flag = false;
			for(unsigned int i = 0; i < val_vector.size(); i++)
			{
				if (val_vector[i].first != new_value)
				{
					val_vector[i].second.push_back(false);

				}
				else 
				{
					val_vector[i].second.push_back(true);
					flag = true;
				}
			}
			if (flag == false)
			{
				/*for(unsigned int i = 0; i < val_vector.size(); i++)
				{
					bit_vector.push_back(false);
				}*/
				
				bit_vector.resize(val_vector[0].second.size()-1, false);
				bit_vector.push_back(true);
				val_vector.push_back(std::make_pair(new_value, bit_vector));
			}
		}
		return false;
	}

	template <typename T> 
	template <typename InputIterator>
	bool BitVectorCompressedColumn<T>::insert(InputIterator , InputIterator ){
		
		return true;
	}

	template<class T>
	const boost::any BitVectorCompressedColumn<T>::get(TID){

		return boost::any();
	}

	template<class T>
	void BitVectorCompressedColumn<T>::print() const throw(){

	}
	template<class T>
	size_t BitVectorCompressedColumn<T>::size() const throw(){
		size_t n = 0;
		n = val_vector[0].second.size();
		return n;
	}
	template<class T>
	const ColumnPtr BitVectorCompressedColumn<T>::copy() const{

		return ColumnPtr();
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::update(TID , const boost::any& ){
		return false;
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::update(PositionListPtr , const boost::any& ){
		return false;		
	}
	
	template<class T>
	bool BitVectorCompressedColumn<T>::remove(TID){
		return false;	
	}
	
	template<class T>
	bool BitVectorCompressedColumn<T>::remove(PositionListPtr){
		return false;			
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::clearContent(){
		return false;
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::store(const std::string&){
		return false;
	}
	template<class T>
	bool BitVectorCompressedColumn<T>::load(const std::string&){
		return false;
	}

	template<class T>
	T& BitVectorCompressedColumn<T>::operator[](const int index){
		static T t;
		for(unsigned int i = 0; i < val_vector.size(); i++)
		{
			if(val_vector[i].second[index] == true)
			{
				t = val_vector[i].first;
				break;
			}
		}
		return t;
	}

	template<class T>
	unsigned int BitVectorCompressedColumn<T>::getSizeinBytes() const throw(){
		return 0; //return values_.capacity()*sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

