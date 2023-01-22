#!/usr/bin/perl


#####################################
#
#
#
#	Created On: 01/16/23 - 
#
#
#####################################

use strict;
use Data::Dumper;

$Data::Dumper::Sortkey = 1;

#use lib qw( /it_dir/shared );
use lib qw( /home/lentzm/bin );
require qw( accounts.pl );

my $FOUNDATION_CN = DB_CONNECT( DB => 'CASSOC' );
my $RACKSPACE_CN = DB_CONNECT( DB => 'FOUNDATION' );

$FOUNDATION_CN->do('use Cas_SOC');

$FOUNDATION_CN->{ 'FetchHashKeyName' } = 'NAME_uc';
$RACKSPACE_CN->{ 'FetchHashKeyName' } = 'NAME_uc';

my $RESULTS_AREF;
truncate_table( 'foundation_jobs' );
$RESULTS_AREF = get_table_data();
$RESULTS_AREF = assign_job_type( $RESULTS_AREF );
$RESULTS_AREF = insert_test_data( $RESULTS_AREF );

#get the gl_division data
truncate_table( 'gl_divisions' );
$RESULTS_AREF = get_gl_divisions();
insert_gl_divisions( $RESULTS_AREF );



sub truncate_table {
	
	my $table_name = shift;
	
	my $sql = qq{ truncate table $table_name };
	my $sth = $RACKSPACE_CN->prepare( $sql );
	
	$sth->execute( );
	
}

sub get_table_data {

	my $sql = q{select distinct 
					a.job_no,
					a.description,
					a.project_manager_no,
					b.name,
					b.sort_name,
					a.job_start_date,
					a.certified_start_date,
					a.completion_date,
					a.job_status
				from 
					Cas_SOC.dbo.jobs a,
					Cas_SOC.dbo.customers b
				where 
					a.customer_no = b.customer_no
	};

	my $sth = $FOUNDATION_CN->prepare( $sql ) or die "Can't prepare statement: $DBI::errstr";
	$sth->execute();
	return $sth->fetchall_arrayref({});
}

sub assign_job_type {
	
	my $all_results_aref = shift;
	
	for my $rec ( @$all_results_aref ){
		
		$rec->{ 'JOB_NO' }  =~ s/\s*//g; #get rid of all of the spaces, damn SQL server crap
		
		#if ( $rec->{ 'JOB_NO' } =~ /[a-zA-Z]{3}(\d{4})/ ) {
		if ( $rec->{ 'JOB_NO' } =~ /(\w{3})(\d{4})/ ) {	
			my $length = length $rec->{ 'JOB_NO' };
			
			my $three_digit_identifier = $1;
			my $well_number = $2;
			
			if ( $well_number >= 1000 && $length == 7 ){
				
				if ( $three_digit_identifier =~ /\d{3}/ ){
					$rec->{ 'JOB_TYPE' } = 'JOB';	
				} else {
					$rec->{ 'JOB_TYPE' } = 'WELL';
				}
				
			}
			 
			
		} elsif ( $rec->{ 'JOB_NO' } =~ /^PS.*(\d{3})/i ){
			$rec->{ 'JOB_TYPE' } = 'PIPELINE';
		} elsif ( $rec->{ 'DESCRIPTION' } =~ /YARD/i){
			$rec->{ 'JOB_TYPE' } = 'YARD';
		} else {
			$rec->{ 'JOB_TYPE' } = 'JOB';
		}
		
	}
	
	return $all_results_aref;
	
}


sub insert_test_data
{
	
	my $all_results_aref = shift;
	
	my $sql = q{ insert into foundation_jobs 
				( job_no, job_desc, project_manager, name, sort_name, job_type, job_start_date, certified_start_date, completion_date, job_status ) 
				values 
				( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
			   };
	my $sth = $RACKSPACE_CN->prepare( $sql );
	
	for my $rec ( @$all_results_aref ){
		
		if ( $rec->{ 'DESCRIPTION' } eq '#8 HOLLOW' ){
			$rec->{ 'DESCRIPTION' } = 'HOLLOW #8';
			
		}
		
#		if ( $rec->{ 'JOB_TYPE' } eq 'JOB' && ! $rec->{ 'JOB_START_DATE' } ){
#			next; #skip putting record in database, unless is has a job_start_date
#		}
		
		$sth->execute( $rec->{ 'JOB_NO' }, $rec->{ 'DESCRIPTION' }, $rec->{ 'PROJECT_MANAGER' }, 
					   $rec->{ 'NAME' }, $rec->{ 'SORT_NAME' }, $rec->{ 'JOB_TYPE' },
					   $rec->{ 'JOB_START_DATE' }, $rec->{ 'CERTIFIED_START_DATE' }, $rec->{ 'COMPLETION_DATE' },
					   $rec->{ 'JOB_STATUS' } );
		
	}
	
	return $all_results_aref;
	
	
}


##Get the GL division data
sub get_gl_divisions {

	my $sql = q{ select company_no, division_account_no, description, short_desc, record_status, row_modified_on from Cas_SOC.dbo.gl_divisions };

	my $sth = $FOUNDATION_CN->prepare( $sql ) or die "Can't prepare statement: $DBI::errstr";
	$sth->execute();
	return $sth->fetchall_arrayref({});
}

sub insert_gl_divisions
{
	
	my $all_results_aref = shift;
	
	my $sql = q{ insert into gl_divisions 
				( company_no, division_account_no, description, short_desc, record_status, row_modified_on ) 
				values 
				( ?, ?, ?, ?, ?, ?)
			   };
	my $sth = $RACKSPACE_CN->prepare( $sql );
	
	for my $rec ( @$all_results_aref ){
		
		$sth->execute( $rec->{ 'COMPANY_NO' }, $rec->{ 'DIVISION_ACCOUNT_NO' }, $rec->{ 'DESCRIPTION' }, 
					   $rec->{ 'SHORT_DESC' }, $rec->{ 'RECORD_STATUS' }, $rec->{ 'ROW_MODIFIED_ON' }
					  );
		
	}
	
	return $all_results_aref;
	
	
}


#print Dumper( $RESULTS_AREF );

