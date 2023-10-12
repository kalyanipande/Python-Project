import smtplib
import os
import ast


class Admin_Email_Alert:

    def send_email(self,product_name_lst):
        namespaces = os.getenv('NAMESPACE')
        prod_flag=False
        env="local"
        print("namespace :",namespaces)
        if namespaces is not None:
            if "production" in namespaces:
                prod_flag=True
                env=os.getenv('DATA_CENTER')
            else:
                if "test-validation-" in namespaces:
                    env="test-validation"
                else:
                    env = "asia-staging"
        text = """Hi,
                    As you are an admin, there is a new approval request on ALICE.
                    Please visit ALICE tool to review and approve the request.
                    https://alice-test-validation-staging.americas.abi.dyn.nesc.nokia.net/#/admin/settings"""
        if  "test-validation" in env:
            text = """Hi,
            As you are an admin, there is a new approval request on ALICE.
            Please visit ALICE tool to review and approve the request.
            https://alice-test-validation-staging.americas.abi.dyn.nesc.nokia.net/#/admin/settings"""
        elif "asia-staging" in env:
            text = """Hi,
                        As you are an admin, there is a new approval request on ALICE.
                        Please visit ALICE tool to review and approve the request.
                        https://nokiaaliceqa.ext.net.nokia.com/#/admin/settings"""
        elif "americas" in env:
            text = """Hi,
                      As you are an admin, there is a new approval request on ALICE.
                      Please visit ALICE tool to review and approve the request.
                      https://nokiaaliceamericas.ext.net.nokia.com/#/admin/settings"""
        elif "asia" in env:
            text = """Hi,
                      As you are an admin, there is a new approval request on ALICE.
                      Please visit ALICE tool to review and approve the request.
                      https://nokiaaliceasia.ext.net.nokia.com/#/admin/settings"""
        elif "europe" in env:
            text = """Hi,
                      As you are an admin, there is a new approval request on ALICE.
                      Please visit ALICE tool to review and approve the request.
                      https://nokiaaliceeurope.ext.net.nokia.com/#/admin/settings"""



        SUBJECT = "ALICE-Request for Global Rule/Known Signature/CA Signature approval."
        message = 'Subject: {}\n\n{}'.format(SUBJECT, text)
        # prod_mapping_dict ={"AirScale BSC":["sumit.kapoor@nokia.com", "zameer.patel@nokia.com"],
        #                     "AirScale BTS - 5G":["arun.12.kumar@nokia.com", "peeyush.goyal@nokia.com", "zameer.patel@nokia.com"],
        #                     "AirScale Cloud BTS - 5G":["arun.12.kumar@nokia.com", "zameer.patel@nokia.com"],
        #                     "AirScale RNC":["amardeep.singh@nokia.com", "zameer.patel@nokia.com"],
        #                     "Multicontroller BSC":["sumit.kapoor@nokia.com", "zameer.patel@nokia.com"],
        #                     "Multicontroller RNC":["amardeep.singh@nokia.com", "zameer.patel@nokia.com"],
        #                     "Nokia AirFrame Data Center Solution":["chiranjeev.negi@nokia.com", "zameer.patel@nokia.com"],
        #                     "Nokia Cloud Infrastructure Solution":["chiranjeev.negi@nokia.com", "zameer.patel@nokia.com"],
        #                     "Single RAN":["arun.12.kumar@nokia.com", "peeyush.goyal@nokia.com", "adrian.1.burian@nokia.com", "bill.deweese@nokia.com", "mikko.rantala@nokia.com", "zameer.patel@nokia.com"],
        #                     "WCDMA RNC":["amardeep.singh@nokia.com", "zamveer.patel@nokia.com"],
        #                     "9500 MPR (Microwave Packet Radio)":["zameer.patel@nokia.com"],
        #                     "Airscale BTS LTE":["zameer.patel@nokia.com"],
        #                     "Flexi BSC":["zameer.patel@nokia.com"],
        #                     "Flexi Multiradio BTS LTE":[" zameer.patel@nokia.com"],
        #                     "Flexi Zone BTS":["zameer.patel@nokia.com"],
        #                     "Flexi Zone BTS TD-LTE":["zameer.patel@nokia.com"]}

        for prod_name in product_name_lst:
            # if prod_flag:
            #     recepient = ["zameer.patel@nokia.com"]
            #     if prod_name in prod_mapping_dict.keys():
            #         recepient=prod_mapping_dict[prod_name]
            # else:
            #     recepient = ["rishabh.trivedi.ext@nokia.com","hemant.singh@nokia.com","shalini.1.verma@nokia.com","rui.2.martins@nokia.com"]
            # os.getenv('DEFAULT_EMAIL_MAPPING_KEY')

            to_recipient = ast.literal_eval(os.getenv('DEFAULT_EMAIL_MAPPING_KEY'))

            if prod_name in ast.literal_eval(os.getenv('PRODUCT_EMAIL_MAPPING_KEY')).keys():
                print("product name matched")
                to_recipient= ast.literal_eval(os.getenv('PRODUCT_EMAIL_MAPPING_KEY'))[prod_name]

            with smtplib.SMTP("mailrelay.int.nokia.com", 25) as server:  # mailrelay.int.nokia.com
                server.sendmail("I_EXT_ALICE_DE_SUPPORT@internal.nsn.com", to_recipient, message, )
